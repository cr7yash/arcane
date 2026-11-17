package prune

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libtnb/sqlite"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"kit.getarcane.app/docker/streams/bus"

	"getarcane.dev/arcane/v3/internal/activity"
	"getarcane.dev/arcane/v3/internal/config"
	"getarcane.dev/arcane/v3/internal/database"
	"getarcane.dev/arcane/v3/internal/docker"
	"getarcane.dev/arcane/v3/internal/event"
	"getarcane.dev/arcane/v3/internal/imageupdate"
	"getarcane.dev/arcane/v3/internal/registry"
	"getarcane.dev/arcane/v3/internal/settings"
	"getarcane.dev/arcane/v3/internal/volume"
	"getarcane.dev/arcane/v3/pkg/httpx"
	"getarcane.dev/types/v3"
)

func TestPruneVolumesSkipsBackupVolume(t *testing.T) {
	var mu sync.Mutex
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/system/df"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Volumes":[`+
				`{"Name":"orphan","Driver":"local","UsageData":{"RefCount":0,"Size":100}},`+
				`{"Name":"arcane-backups","Driver":"local","UsageData":{"RefCount":0,"Size":200}}]}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[]`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/volumes/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Name":"`+path.Base(r.URL.Path)+`","Driver":"local"}`)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/volumes/"):
			mu.Lock()
			deleted = append(deleted, path.Base(r.URL.Path))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	dialer := &net.Dialer{}
	dockerClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
		client.WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerClient.Close() })

	gormDB, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&event.Event{}))
	db := &database.DB{DB: gormDB}

	dockerService := newSettingsDockerService(t, dockerClient)
	volumeService := volume.NewVolumeService(volume.VolumeServiceParams{
		Config:        &config.Config{},
		DB:            db,
		DockerService: dockerService,
		EventService:  event.NewEventService(db, &config.Config{}, nil),
	})
	service := NewService(ServiceParams{
		Location:      time.UTC,
		DockerService: dockerService,
		VolumeService: volumeService,
	})

	result, err := service.pruneVolumes(t.Context(), types.PruneVolumesOptions{Mode: types.PruneVolumeModeAll})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan"}, result.VolumesDeleted)
	require.EqualValues(t, 100, result.VolumeSpaceReclaimed)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"orphan"}, deleted)
}

func TestGetDiskUsageSumsReclaimableResources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/system/df") {
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{`+
			`"Images":[`+
			`{"Id":"sha256:unused","Containers":0,"Size":500,"SharedSize":100},`+
			`{"Id":"sha256:used","Containers":1,"Size":900,"SharedSize":0},`+
			`{"Id":"sha256:unknown","Containers":0,"Size":700,"SharedSize":-1}],`+
			`"Containers":[`+
			`{"Id":"stopped","State":"exited","SizeRw":30},`+
			`{"Id":"running","State":"running","SizeRw":70}],`+
			`"Volumes":[`+
			`{"Name":"orphan","Driver":"local","UsageData":{"RefCount":0,"Size":100}},`+
			`{"Name":"mounted","Driver":"local","UsageData":{"RefCount":1,"Size":50}},`+
			`{"Name":"arcane-backups","Driver":"local","UsageData":{"RefCount":0,"Size":200}}],`+
			`"BuildCache":[`+
			`{"ID":"idle","InUse":false,"Size":10},`+
			`{"ID":"busy","InUse":true,"Size":20}]}`)
	}))
	t.Cleanup(server.Close)

	dialer := &net.Dialer{}
	dockerClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
		client.WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerClient.Close() })

	gormDB, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&event.Event{}))
	db := &database.DB{DB: gormDB}

	dockerService := newSettingsDockerService(t, dockerClient)
	volumeService := volume.NewVolumeService(volume.VolumeServiceParams{
		Config:        &config.Config{},
		DB:            db,
		DockerService: dockerService,
		EventService:  event.NewEventService(db, &config.Config{}, nil),
	})
	service := NewService(ServiceParams{
		Location:      time.UTC,
		DockerService: dockerService,
		VolumeService: volumeService,
	})

	usage, err := service.GetDiskUsage(t.Context())
	require.NoError(t, err)
	require.Equal(t, types.SystemDiskUsage{
		UnusedImagesBytes:      400,
		BuildCacheBytes:        10,
		UnusedVolumesBytes:     100,
		StoppedContainersBytes: 30,
	}, usage)
}

func newSettingsDockerService(t *testing.T, dockerClient *client.Client) *docker.DockerClientService {
	t.Helper()

	gormDB, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&settings.SettingVariable{}))
	db := &database.DB{DB: gormDB}
	settingsService, err := settings.NewSettingsService(settings.SettingsServiceParams{
		Ctx:    t.Context(),
		DB:     db,
		Config: &config.Config{},
		Env:    settings.Env{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, settingsService.Stop(context.WithoutCancel(t.Context()))) })
	return docker.NewDockerClientService(t.Context(), db, settingsService, bus.Config{}).WithClient(dockerClient)
}

func TestPreparePrunePlanRequestsOnlySelectedUsageTypes(t *testing.T) {
	var mu sync.Mutex
	var requested []string
	var removed []string
	var taggedRemoved, sharedRemoved bool
	service := newPruneTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/system/df"):
			mu.Lock()
			requested = r.URL.Query()["type"]
			layersSize := 1400
			images := `{"Id":"sha256:dangling","Containers":0,"Size":500,"SharedSize":0}`
			if !taggedRemoved {
				images += `,{"Id":"sha256:tagged","Containers":0,"Size":900,"SharedSize":700,"RepoTags":["app:latest","app:stable"]}`
			} else {
				layersSize -= 200
			}
			if !sharedRemoved {
				images += `,{"Id":"sha256:shared","Containers":0,"Size":700,"SharedSize":-1}`
			} else {
				layersSize -= 700
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"LayersSize":%d,"Images":[%s]}`, layersSize, images)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[]`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/images/sha256:dangling/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Id":"sha256:dangling"}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/images/sha256:shared/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Id":"sha256:shared"}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/images/sha256:tagged/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Id":"sha256:tagged","RepoTags":["app:latest","app:stable"]}`)
		case r.Method == http.MethodGet && (strings.HasSuffix(r.URL.Path, "/images/app:latest/json") || strings.HasSuffix(r.URL.Path, "/images/app:stable/json")):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Id":"sha256:tagged"}`)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/app:latest"):
			mu.Lock()
			removed = append(removed, "app:latest")
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[{"Untagged":"app:latest"}]`)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/app:stable"):
			mu.Lock()
			removed = append(removed, "app:stable")
			taggedRemoved = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[{"Untagged":"app:stable"},{"Deleted":"sha256:tagged"}]`)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/sha256:shared"):
			mu.Lock()
			removed = append(removed, "sha256:shared")
			sharedRemoved = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[{"Deleted":"sha256:shared"}]`)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/sha256:dangling"):
			mu.Lock()
			removed = append(removed, "sha256:dangling")
			mu.Unlock()
			http.Error(w, "already removed", http.StatusNotFound)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/sha256:tagged"):
			mu.Lock()
			removed = append(removed, "sha256:tagged")
			mu.Unlock()
			http.Error(w, "already removed", http.StatusNotFound)
		default:
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))

	plan, err := service.preparePrunePlan(t.Context(), types.PruneAllRequest{
		Images: &types.PruneImagesOptions{Mode: types.PruneImageModeDangling},
	})
	require.NoError(t, err)
	require.Equal(t, []pruneResource{{Kind: "image", ID: "sha256:dangling", Size: 500}, {Kind: "image", ID: "sha256:shared", SizeUnknown: true}}, plan.Resources)
	mu.Lock()
	require.Equal(t, []string{"image"}, requested)
	mu.Unlock()

	allPlan, err := service.preparePrunePlan(t.Context(), types.PruneAllRequest{
		Images: &types.PruneImagesOptions{Mode: types.PruneImageModeAll},
	})
	require.NoError(t, err)
	require.Equal(t, []pruneResource{
		{Kind: "image", ID: "sha256:dangling", Size: 500},
		{Kind: "image", ID: "sha256:tagged", Names: []string{"app:latest", "app:stable"}, Size: 200, SizeUnknown: true},
		{Kind: "image", ID: "sha256:shared", SizeUnknown: true},
	}, allPlan.Resources)

	imageDB, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, imageDB.AutoMigrate(&imageupdate.ImageUpdateRecord{}))
	service.imageUpdateService = imageupdate.NewImageUpdateService(imageupdate.ImageUpdateServiceParams{
		DB:              &database.DB{DB: imageDB},
		RegistryService: &registry.ContainerRegistryService{},
	})

	result, err := service.executePrunePlan(t.Context(), allPlan, types.SchedulerRun{}, "", false)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, []string{"sha256:tagged", "sha256:shared"}, result.ImagesDeleted)
	require.EqualValues(t, 900, result.ImageSpaceReclaimed)
	require.EqualValues(t, 900, result.SpaceReclaimed)
	require.False(t, result.ImageSpaceReclaimedUnknown)
	require.False(t, result.SpaceReclaimedUnknown)
	mu.Lock()
	require.Equal(t, []string{"sha256:dangling", "app:latest", "app:stable", "sha256:shared"}, removed)
	mu.Unlock()

	for _, unknownCase := range []struct {
		name       string
		sharedSize int
	}{
		{name: "sha256:unknown", sharedSize: -1},
		{name: "sha256:shared-zero", sharedSize: 700},
	} {
		t.Run("unattributable size "+unknownCase.name, func(t *testing.T) {
			unknownService := newPruneTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/system/df"):
					_, _ = fmt.Fprintf(w, `{"Images":[{"Id":"%s","Containers":0,"Size":700,"SharedSize":%d},{"Id":"sha256:known","Containers":0,"Size":200,"SharedSize":0}]}`, unknownCase.name, unknownCase.sharedSize)
				case r.Method == http.MethodGet && (strings.HasSuffix(r.URL.Path, "/images/"+unknownCase.name+"/json") || strings.HasSuffix(r.URL.Path, "/images/sha256:known/json")):
					_, _ = fmt.Fprintf(w, `{"Id":"%s"}`, path.Base(path.Dir(r.URL.Path)))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
					_, _ = fmt.Fprint(w, `[]`)
				case r.Method == http.MethodDelete && (strings.HasSuffix(r.URL.Path, "/images/"+unknownCase.name) || strings.HasSuffix(r.URL.Path, "/images/sha256:known")):
					_, _ = fmt.Fprintf(w, `[{"Deleted":"%s"}]`, path.Base(r.URL.Path))
				default:
					http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
				}
			}))
			unknownService.imageUpdateService = service.imageUpdateService
			unknownPlan, err := unknownService.preparePrunePlan(t.Context(), types.PruneAllRequest{
				Images: &types.PruneImagesOptions{Mode: types.PruneImageModeAll},
			})
			require.NoError(t, err)
			require.Equal(t, []pruneResource{
				{Kind: "image", ID: unknownCase.name, SizeUnknown: true},
				{Kind: "image", ID: "sha256:known", Size: 200},
			}, unknownPlan.Resources)
			unknownResult, err := unknownService.executePrunePlan(t.Context(), unknownPlan, types.SchedulerRun{}, "", false)
			require.NoError(t, err)
			require.Equal(t, []string{unknownCase.name, "sha256:known"}, unknownResult.ImagesDeleted)
			require.EqualValues(t, 200, unknownResult.ImageSpaceReclaimed)
			require.EqualValues(t, 200, unknownResult.SpaceReclaimed)
			require.True(t, unknownResult.ImageSpaceReclaimedUnknown)
			require.True(t, unknownResult.SpaceReclaimedUnknown)
		})
	}

	for _, pulledID := range []string{"sha256:pulled", "sha256:shared-zero"} {
		t.Run("concurrent pull "+pulledID+" does not prove zero reclamation", func(t *testing.T) {
			var mu sync.Mutex
			removed := false
			concurrentService := newPruneTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/system/df"):
					mu.Lock()
					wasRemoved := removed
					mu.Unlock()
					if wasRemoved {
						_, _ = fmt.Fprintf(w, `{"LayersSize":700,"Images":[{"Id":"%s","Containers":0,"Size":700,"SharedSize":0}]}`, pulledID)
					} else {
						_, _ = fmt.Fprint(w, `{"LayersSize":700,"Images":[{"Id":"sha256:shared-zero","Containers":0,"Size":700,"SharedSize":700}]}`)
					}
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/images/sha256:shared-zero/json"):
					_, _ = fmt.Fprint(w, `{"Id":"sha256:shared-zero"}`)
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
					_, _ = fmt.Fprint(w, `[]`)
				case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/images/sha256:shared-zero"):
					mu.Lock()
					removed = true
					mu.Unlock()
					_, _ = fmt.Fprint(w, `[{"Deleted":"sha256:shared-zero"}]`)
				default:
					http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
				}
			}))
			concurrentService.imageUpdateService = service.imageUpdateService
			concurrentPlan, err := concurrentService.preparePrunePlan(t.Context(), types.PruneAllRequest{
				Images: &types.PruneImagesOptions{Mode: types.PruneImageModeAll},
			})
			require.NoError(t, err)
			concurrentResult, err := concurrentService.executePrunePlan(t.Context(), concurrentPlan, types.SchedulerRun{}, "", false)
			require.NoError(t, err)
			require.Equal(t, []string{"sha256:shared-zero"}, concurrentResult.ImagesDeleted)
			require.Zero(t, concurrentResult.ImageSpaceReclaimed)
			require.True(t, concurrentResult.ImageSpaceReclaimedUnknown)
			require.True(t, concurrentResult.SpaceReclaimedUnknown)
		})
	}
}

func TestPreparePrunePlanMarksDiskUsageFailure(t *testing.T) {
	service := newPruneTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "df failed", http.StatusInternalServerError)
	}))

	_, err := service.preparePrunePlan(t.Context(), types.PruneAllRequest{
		Images: &types.PruneImagesOptions{Mode: types.PruneImageModeDangling},
	})
	require.ErrorIs(t, err, errDiskUsageUnavailable)
}

func newPruneTestService(t *testing.T, handler http.Handler) *Service {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	dialer := &net.Dialer{}
	dockerClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
		client.WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerClient.Close() })

	return NewService(ServiceParams{
		Location:      time.UTC,
		DockerService: newSettingsDockerService(t, dockerClient),
	})
}

func TestPruneVolumesFallsBackToVolumeListWithoutDiskUsage(t *testing.T) {
	service, deleted := newVolumeFallbackService(t)

	result, err := service.pruneVolumes(t.Context(), types.PruneVolumesOptions{Mode: types.PruneVolumeModeAll})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan"}, result.VolumesDeleted)
	require.Equal(t, []string{"orphan"}, deleted())
}

func TestReconcileScheduledPruneReportsInterruptedFallbackWithoutReplaying(t *testing.T) {
	service, deleted := newVolumeFallbackService(t)

	outcome, err := service.ReconcileScheduledPrune(t.Context(), types.SchedulerRun{
		EnvironmentID: "0",
		Outcome: types.SchedulerOutcome{
			Targets: []types.TargetOutcome{{
				ResourceType: "prune_fallback",
				ID:           "prune-fallback",
				Status:       types.RunStatusRunning,
				ActivityID:   "activity-1",
			}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.RunStatusFailed, outcome.Status)
	require.Equal(t, "activity-1", outcome.ActivityID)
	require.Empty(t, deleted())
}

func newVolumeFallbackService(t *testing.T) (*Service, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/system/df"):
			http.Error(w, "df failed", http.StatusInternalServerError)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/volumes"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Volumes":[`+
				`{"Name":"orphan","Driver":"local"},`+
				`{"Name":"arcane-backups","Driver":"local"},`+
				`{"Name":"helper","Driver":"local","Labels":{"com.getarcaneapp.internal.resource":"true"}}]}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[]`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/volumes/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"Name":"`+path.Base(r.URL.Path)+`","Driver":"local"}`)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/volumes/"):
			mu.Lock()
			deleted = append(deleted, path.Base(r.URL.Path))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	dialer := &net.Dialer{}
	dockerClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
		client.WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerClient.Close() })

	gormDB, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&event.Event{}, &activity.Activity{}, &activity.ActivityMessage{}))
	db := &database.DB{DB: gormDB}

	dockerService := newSettingsDockerService(t, dockerClient)
	service := NewService(ServiceParams{
		Location:        time.UTC,
		DockerService:   dockerService,
		ActivityService: activity.NewActivityService(db, nil),
		VolumeService: volume.NewVolumeService(volume.VolumeServiceParams{
			Config:        &config.Config{},
			DB:            db,
			DockerService: dockerService,
			EventService:  event.NewEventService(db, &config.Config{}, nil),
		}),
	})
	return service, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(deleted)
	}
}

func diskUsageScopeContext(t *testing.T, bodies map[string]any) context.Context {
	t.Helper()
	return httpx.WithEnvironmentScope(t.Context(), &httpx.EnvironmentScope{
		IDs:     []string{"a", "b", "c"},
		Request: httptest.NewRequest(http.MethodGet, "/api/agents/a,b,c/system/df", http.NoBody),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, ok := bodies[strings.Split(strings.TrimPrefix(r.URL.Path, "/api/agents/"), "/")[0]]
			if !ok {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"detail":"environment offline"}`))
				return
			}
			_ = json.MarshalWrite(w, body)
		}),
		Resolve: func(_ context.Context, id string) (types.EnvironmentRef, error) {
			return types.EnvironmentRef{ID: id, Name: "Env " + id}, nil
		},
	})
}

func TestAggregateDiskUsageSumsEnvironments(t *testing.T) {
	ctx := diskUsageScopeContext(t, map[string]any{
		"a": types.ApiResponse[types.SystemDiskUsage]{Success: true, Data: types.SystemDiskUsage{
			UnusedImagesBytes:      1,
			BuildCacheBytes:        2,
			UnusedVolumesBytes:     3,
			StoppedContainersBytes: 4,
		}},
		"b": types.ApiResponse[types.SystemDiskUsage]{Success: true, Data: types.SystemDiskUsage{
			UnusedImagesBytes:      10,
			BuildCacheBytes:        20,
			UnusedVolumesBytes:     30,
			StoppedContainersBytes: 40,
		}},
	})

	body, err := (&Service{}).AggregateDiskUsage(ctx)
	require.NoError(t, err)

	require.True(t, body.Success)
	require.Equal(t, types.SystemDiskUsage{
		UnusedImagesBytes:      11,
		BuildCacheBytes:        22,
		UnusedVolumesBytes:     33,
		StoppedContainersBytes: 44,
	}, body.Data)
	require.Len(t, body.Environments, 2)
	require.Equal(t, []types.EnvironmentFailure{{Environment: types.EnvironmentRef{ID: "c", Name: "c"}, Message: "environment offline"}}, body.FailedEnvironments)
}

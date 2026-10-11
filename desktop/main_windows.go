//go:build windows

package main

import (
	"embed"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	desktopapp "github.com/Paulkm2006/p2pfs/desktop/internal/app"
	"github.com/wailsapp/wails/v3/pkg/application"
)

//go:embed frontend
var assets embed.FS

func main() {
	configDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal(err)
	}
	core, err := desktopapp.New(filepath.Join(configDir, "p2pfs", "desktop.json"))
	if err != nil {
		log.Fatal(err)
	}
	frontend, err := fs.Sub(assets, "frontend")
	if err != nil {
		log.Fatal(err)
	}
	service := &DesktopService{core: core}
	app := application.New(application.Options{
		Name: "p2pfs", Description: "P2P 分片存储与下载",
		Services:   []application.Service{application.NewService(service)},
		Assets:     application.AssetOptions{Handler: application.BundledAssetFileServer(frontend)},
		OnShutdown: func() { _ = core.Disconnect() },
	})
	app.Window.NewWithOptions(application.WebviewWindowOptions{
		Title: "p2pfs · 分片协作", Width: 1220, Height: 820, MinWidth: 900, MinHeight: 650, URL: "/",
		BackgroundColour: application.NewRGB(246, 248, 252),
	})
	if err = app.Run(); err != nil {
		log.Fatal(err)
	}
}

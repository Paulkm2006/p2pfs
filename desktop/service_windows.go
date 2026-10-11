//go:build windows

package main

import (
	desktopapp "github.com/Paulkm2006/p2pfs/desktop/internal/app"
	"github.com/Paulkm2006/p2pfs/internal/client"
	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/wailsapp/wails/v3/pkg/application"
)

type DesktopService struct{ core *desktopapp.Service }

func (s *DesktopService) Status() desktopapp.Status                    { return s.core.Status() }
func (s *DesktopService) Connect(cfg desktopapp.Config) error          { return s.core.Connect(cfg) }
func (s *DesktopService) Disconnect() error                            { return s.core.Disconnect() }
func (s *DesktopService) Files(query string) ([]model.FileInfo, error) { return s.core.Files(query) }
func (s *DesktopService) Publish(path string) (model.Manifest, error)  { return s.core.Publish(path) }
func (s *DesktopService) Download(id, path string) (string, error)     { return s.core.Download(id, path) }
func (s *DesktopService) TaskAction(id, action string) error           { return s.core.TaskAction(id, action) }
func (s *DesktopService) Retain(id string, indices []int) error        { return s.core.Retain(id, indices) }
func (s *DesktopService) Unshare(id string, deleteCache bool) error {
	return s.core.Unshare(id, deleteCache)
}
func (s *DesktopService) Storage() (desktopapp.Storage, error) { return s.core.Storage() }
func (s *DesktopService) ChoosePublishFile() (string, error) {
	return application.Get().Dialog.OpenFile().SetTitle("选择要发布的文件（最大 1 GiB）").CanChooseDirectories(false).CanChooseFiles(true).PromptForSingleSelection()
}
func (s *DesktopService) ChooseDataDirectory() (string, error) {
	return application.Get().Dialog.OpenFile().SetTitle("选择本机分片与任务数据目录").CanChooseDirectories(true).CanChooseFiles(false).PromptForSingleSelection()
}
func (s *DesktopService) ChooseDestination(name string) (string, error) {
	// Remote manifest names are display data, never directory paths.
	name = desktopapp.SafeFilename(name)
	return application.Get().Dialog.SaveFileWithOptions(&application.SaveFileDialogOptions{Title: "保存下载文件（请选择新文件名）", Filename: name, CanCreateDirectories: true, AllowOtherFileTypes: true}).PromptForSingleSelection()
}

func (s *DesktopService) LocalFiles() ([]client.LocalFile, error) { return s.core.LocalFiles() }

func (s *DesktopService) ExportStatistics() (string, error) {
	path, err := application.Get().Dialog.SaveFileWithOptions(&application.SaveFileDialogOptions{Title: "导出诊断统计（不包含会话凭据）", Filename: "p2pfs-statistics.json", CanCreateDirectories: true, AllowOtherFileTypes: true}).PromptForSingleSelection()
	if err != nil || path == "" {
		return "", err
	}
	if err = s.core.ExportStatistics(path); err != nil {
		return "", err
	}
	return path, nil
}

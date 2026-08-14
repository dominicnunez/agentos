package main

import (
	"fmt"
	"io"
	"os"
)

func readUnchangedBoundedFile(path string, before os.FileInfo, maximum int64, label string) ([]byte, error) {
	if before == nil || maximum <= 0 || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > maximum || label == "" {
		return nil, fmt.Errorf("%s file snapshot is invalid", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%s changed while it was opened", label)
	}
	body, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(body)) != before.Size() {
		clearBytes(body)
		return nil, fmt.Errorf("%s changed while it was read", label)
	}
	return body, nil
}

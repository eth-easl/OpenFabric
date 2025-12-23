package common

import (
	"os"
)

func RemoveDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Logger.Info("path does not exist: " + path + ". Skipping...")
		return nil
	}
	return os.RemoveAll(path)
}

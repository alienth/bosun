package collectors

import "fmt"

func WatchProcesses(procs []string) error {
	if len(procs) == 0 {
		return nil
	}
	return fmt.Errorf("process watching not implemented on Darwin")
}

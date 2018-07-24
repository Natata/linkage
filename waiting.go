package linkage

import (
	"fmt"
	"time"
)

// Waiting define the rule of wait time between each retry
type Waiting func() error

// WaitFactor generate waiting function
func WaitFactor(init int, grow int, maxRetry int) Waiting {
	if init < 1 {
		init = 1
	}
	if grow < 1 {
		init = 1
	}

	wt := init
	attempt := 0
	return func() error {
		if attempt == maxRetry {
			return fmt.Errorf("reach max attempt")
		}

		wt = wt * grow
		time.Sleep(time.Duration(wt) * time.Second)
		attempt++
		return nil
	}
}

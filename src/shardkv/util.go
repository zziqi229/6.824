package shardkv

import "log"

// Debugging
const Debug_level = 999

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug_level <= level {
		log.Printf(format, a...)
		// t := time.Now()
		// timeinfo := fmt.Sprintf("[%02d:%02d:%02d.%3d]", t.Local().Hour(), t.Local().Minute(), t.Local().Second(), t.Local().Nanosecond())
		// fmt.Printf(timeinfo+format+"\n", a...)
	}
	return
}
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func ifCond(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	} else {
		return b
	}
}

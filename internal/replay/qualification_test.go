package replay

import (
	"os"
	"testing"
)

func TestFullReplayQualification(t *testing.T) {
	if os.Getenv("RAILYARD_FULL_REPLAY") != "1" {
		t.Skip("set RAILYARD_FULL_REPLAY=1 to run full replay qualification")
	}
	t.Fatal("in-process qualification cannot prove clean processes; run go run ./test/replay --output <new-directory>")
}

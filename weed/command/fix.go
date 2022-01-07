package command

import (
	"os"
	"path"
	"strconv"
	"regexp"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	cmdFix.Run = runFix // break init cycle
}

var cmdFix = &Command{
	UsageLine: "fix /path/to/volume.dat [/path/to/another/volume.dat]",
	Short:     "run to recreate index .idx files of volume .dat files",
	Long: `Fix runs the SeaweedFS fix command on volume .dat files to re-create their index .idx files.

  `,
}

type VolumeFileScanner4Fix struct {
	version needle.Version
	nm      *needle_map.MemDb
}

func (scanner *VolumeFileScanner4Fix) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil

}

func (scanner *VolumeFileScanner4Fix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4Fix) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	glog.V(2).Infof("key %d offset %d size %d disk_size %d compressed %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed())
	if n.Size.IsValid() {
		pe := scanner.nm.Set(n.Id, types.ToOffset(offset), n.Size)
		glog.V(2).Infof("saved %d with error %v", n.Size, pe)
	} else {
		glog.V(2).Infof("skipping deleted file ...")
		return scanner.nm.Delete(n.Id)
	}
	return nil
}

func runFix(cmd *Command, args []string) bool {
	//args := []string{"/path/to/data_1.dat", "/1.dat", "/path/to/2.dat", "./collection_24.dat", "./path/to/42.dat"}
	re := regexp.MustCompile(`^(?P<path>\.?/(?:.+/)*)?(?P<filename>(?:(?P<collection>.+?)_)?(?P<volumeId>[\d]+))\.(?P<extension>.+)$`)
	for _, arg := range args {
		
		if _, err := os.Stat(arg); os.IsNotExist(err) {
			glog.V(0).Infof("skipping nonextant file: %s", arg)
			continue;
		}

		m := re.FindStringSubmatch(arg)

		glog.V(4).Infof("weed fix argument: \"%s\"\n", arg)
		glog.V(4).Infof("subexpressions/captured groups: %#v\n", re.SubexpNames())
		glog.V(4).Infof("resulting Match: %#v\n", m)

		if m[4] == "" {
			glog.V(2).Infof("volumeId did parse as empty instead of a sequence of digits, unexpected file naming and/or parsing issue")
			continue;
		}

		fixVolumePath := m[1]
		fixFilename := m[2]
		fixVolumeCollection := m[3]
		fixVolumeId,_ := strconv.Atoi(m[4])
		fixExtension := m[5]

		if fixExtension != "dat" {
			glog.V(2).Infof("extension did not parse as dat: %s , skipping %s", fixExtension, arg)
			continue;
		}

		if fixFilename == "" {
			glog.V(2).Infof("filename did parse as empty, unexpected file naming and/or parsing issue")
			continue;
		}


		indexFileName := path.Join(util.ResolvePath(fixVolumePath), fixFilename+".idx")

		nm := needle_map.NewMemDb()
		defer nm.Close()

		vid := needle.VolumeId(fixVolumeId)
		scanner := &VolumeFileScanner4Fix{
			nm: nm,
		}

		if err := storage.ScanVolumeFile(util.ResolvePath(fixVolumePath), fixVolumeCollection, vid, storage.NeedleMapInMemory, scanner); err != nil {
			glog.Fatalf("scan .dat File: %v", err)
			os.Remove(indexFileName)
		}

		if err := nm.SaveToIdx(indexFileName); err != nil {
			glog.Fatalf("save to .idx File: %v", err)
			os.Remove(indexFileName)
		}
	}
	return true
}

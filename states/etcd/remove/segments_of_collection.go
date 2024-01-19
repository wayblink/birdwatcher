package remove

import (
	"fmt"
	_ "github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentsOfCollectionCommand returns remove segment command.
func SegmentsOfCollectionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segments",
		Short: "Remove segments from meta with specified collection id, state",
		Run: func(cmd *cobra.Command, args []string) {
			//targetSegmentID, err := cmd.Flags().GetInt64("segment")
			//if err != nil {
			//	fmt.Println(err.Error())
			//	return
			//}
			targetCollectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			targetState, err := cmd.Flags().GetString("state")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segments, err := common.ListSegments(cli, basePath, func(segmentInfo *datapb.SegmentInfo) bool {
				return segmentInfo.GetCollectionID() == targetCollectionID &&
					segmentInfo.GetState().String() == targetState
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return
			}

			// dry run, display segment first
			if !run {
				//show.PrintSegmentInfo(segments[0], false)
				fmt.Printf("segment info %v", segments[0])
				return
			}

			for _, segment := range segments {
				info := segment
				backupSegmentInfo(info)
				fmt.Println("[WARNING] about to remove segment from etcd")
				err = common.RemoveSegment(cli, basePath, info)
				if err != nil {
					fmt.Printf("Remove segment %d from Etcd failed, err: %s\n", info.ID, err.Error())
					return
				}
				fmt.Printf("Remove segment %d from etcd succeeds.\n", info.GetID())
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segment from meta")
	cmd.Flags().Int64("collection", 0, "collection id to remove")
	cmd.Flags().String("state", "Dropped", "segment state to remove")
	return cmd
}

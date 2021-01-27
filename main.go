package main

import (
	"github.com/spf13/cobra"
	//	"github.com/spf13/pflag"
	"github.com/tvanomr/kafkatool/flagtypes"
)

var hostPort = flagtypes.HostPort{Host: "localhost", Port: 9092}

func main() {
	rootCmd := &cobra.Command{Use: "cmd"}
	rootCmd.PersistentFlags().VarP(&hostPort, "address", "a", "kafka server address")
	topicCmd := &cobra.Command{Use: "topic", Aliases: []string{"t"}}
	rootCmd.AddCommand(topicCmd)
	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicModCmd)
	rootCmd.AddCommand(readCmd)
	rootCmd.Execute()
}

package cmd

import (
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize the system, create the database and the config file",
	Run: func(cmd *cobra.Command, args []string) {

	}}
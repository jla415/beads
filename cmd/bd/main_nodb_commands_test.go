package main

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestShouldSkipDBInitForCommand_SyncScope(t *testing.T) {
	root := &cobra.Command{Use: "bd"}

	topSync := &cobra.Command{Use: "sync"}
	root.AddCommand(topSync)

	federation := &cobra.Command{Use: "federation"}
	federationSync := &cobra.Command{Use: "sync"}
	federation.AddCommand(federationSync)
	root.AddCommand(federation)

	linear := &cobra.Command{Use: "linear"}
	linearSync := &cobra.Command{Use: "sync"}
	linear.AddCommand(linearSync)
	root.AddCommand(linear)

	doctor := &cobra.Command{Use: "doctor"}
	doctorFix := &cobra.Command{Use: "fix"}
	doctor.AddCommand(doctorFix)
	root.AddCommand(doctor)

	if !shouldSkipDBInitForCommand(topSync, noDBCommands) {
		t.Fatal("expected top-level 'bd sync' to skip DB init")
	}
	if shouldSkipDBInitForCommand(federationSync, noDBCommands) {
		t.Fatal("'bd federation sync' must not skip DB init")
	}
	if shouldSkipDBInitForCommand(linearSync, noDBCommands) {
		t.Fatal("'bd linear sync' must not skip DB init")
	}
	if !shouldSkipDBInitForCommand(doctorFix, noDBCommands) {
		t.Fatal("subcommands of no-db parents (doctor) should skip DB init")
	}
}

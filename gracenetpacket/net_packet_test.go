package gracenetpacket

import (
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/vishvananda/netns"
)

func TestListenUDPNamespace(t *testing.T) {
	t.Skip() // Requires root privs to create namespaces

	// Create three namespaces
	ns1, err := netns.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ns1.Close()
	ns2, err := netns.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ns2.Close()
	ns3, err := netns.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ns3.Close()

	addr, err := net.ResolveUDPAddr("udp", ":8000")
	if err != nil {
		t.Fatal(err)
	}

	// Create a UDP socket in each of the namespaces
	app1 := &Net{}
	if _, err := app1.ListenUDPNamespace("udp", addr, ns1); err != nil {
		t.Fatal(err)
	}
	if _, err := app1.ListenUDPNamespace("udp", addr, ns2); err != nil {
		t.Fatal(err)
	}
	if _, err := app1.ListenUDPNamespace("udp", addr, ns3); err != nil {
		t.Fatal(err)
	}
	if len(app1.active) != 3 {
		t.Fatalf("Should have 3 active sockets.  Have %d\n", len(app1.active))
	}

	// Check address already in use if we try to create a socket that already exists
	if _, err := app1.ListenUDPNamespace("udp", addr, ns1); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}
	if _, err := app1.ListenUDPNamespace("udp", addr, ns2); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}
	if _, err := app1.ListenUDPNamespace("udp", addr, ns3); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}

	listeners, err := app1.activeListeners()
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv(envCountKey, strconv.Itoa(len(listeners)))

	// Start a new application and inherit the fd
	app2 := &Net{}
	app2.fdStart = 8
	if _, err := app2.ListenUDPNamespace("udp", addr, ns1); err != nil {
		t.Fatal(err)
	}
	if _, err := app2.ListenUDPNamespace("udp", addr, ns2); err != nil {
		t.Fatal(err)
	}
	if _, err := app2.ListenUDPNamespace("udp", addr, ns3); err != nil {
		t.Fatal(err)
	}
	if len(app2.active) != 3 {
		t.Fatalf("Should have 3 active sockets.  Have %d\n", len(app1.active))
	}
	if _, err := app2.ListenUDPNamespace("udp", addr, ns1); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}
	if _, err := app2.ListenUDPNamespace("udp", addr, ns2); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}
	if _, err := app2.ListenUDPNamespace("udp", addr, ns3); err == nil {
		t.Fatal("Creating a second socket in ns should have failed with 'address already in use'")
	}
}

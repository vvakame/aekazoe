package aekazoe

import (
	"fmt"
	"os"
	"testing"

	"github.com/favclip/testerator"
)

func TestMain(m *testing.M) {
	_, _, err := testerator.SpinUp()
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}
	testerator.DefaultSetup.ResetThreshold = 12

	status := m.Run()

	err = testerator.SpinDown()
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	os.Exit(status)
}

func TestKazoeImpl_PurgeAsyncCount(t *testing.T) {
	_, c, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	kazoe, err := New("foobar")
	if err != nil {
		t.Fatal(err)
	}

	err = kazoe.IncrementAsyncByString(c, "a")
	if err != nil {
		t.Fatal(err)
	}

	err = kazoe.PurgeAsyncCount(c)
	if err != nil {
		t.Fatal(err)
	}

	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		t.Fatal(err)
	}
	closer()

	if v := len(deltaMap); v != 0 {
		t.Error("unexpected:", v)
	}
}

func TestKazoe_IncrementAsyncByString(t *testing.T) {
	_, c, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	kazoe, err := New("foobar")
	if err != nil {
		t.Fatal(err)
	}

	err = kazoe.IncrementAsyncByString(c, "a")
	if err != nil {
		t.Fatal(err)
	}
	err = kazoe.IncrementAsyncByString(c, "a")
	if err != nil {
		t.Fatal(err)
	}
	err = kazoe.IncrementAsyncByString(c, "b")
	if err != nil {
		t.Fatal(err)
	}

	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		t.Fatal(err)
	}
	closer()

	if v := len(deltaMap); v != 2 {
		t.Error("unexpected:", v)
	}
	if v := deltaMap["a"]; v != 2 {
		t.Error("unexpected:", v)
	}
	if v := deltaMap["b"]; v != 1 {
		t.Error("unexpected:", v)
	}

	deltaMap, closer, err = kazoe.CollectDeltaByString(c)
	if err != nil {
		t.Fatal(err)
	}
	closer()
	if v := len(deltaMap); v != 0 {
		t.Error("unexpected:", v)
	}
}

func TestKazoeImpl_DecrementAsyncByString(t *testing.T) {
	_, c, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	kazoe, err := New("foobar")
	if err != nil {
		t.Fatal(err)
	}

	err = kazoe.DecrementAsyncByString(c, "a")
	if err != nil {
		t.Fatal(err)
	}
	err = kazoe.DecrementAsyncByString(c, "a")
	if err != nil {
		t.Fatal(err)
	}
	err = kazoe.DecrementAsyncByString(c, "b")
	if err != nil {
		t.Fatal(err)
	}

	deltaMap, closer, err := kazoe.CollectDeltaByString(c)
	if err != nil {
		t.Fatal(err)
	}
	closer()

	if v := len(deltaMap); v != 2 {
		t.Error("unexpected:", v)
	}
	if v := deltaMap["a"]; v != -2 {
		t.Error("unexpected:", v)
	}
	if v := deltaMap["b"]; v != -1 {
		t.Error("unexpected:", v)
	}

	deltaMap, closer, err = kazoe.CollectDeltaByString(c)
	if err != nil {
		t.Fatal(err)
	}
	closer()
	if v := len(deltaMap); v != 0 {
		t.Error("unexpected:", v)
	}
}

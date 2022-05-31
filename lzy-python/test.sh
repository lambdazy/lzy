#!/bin/bash

nix build -f build.nix test
cat result/test_output

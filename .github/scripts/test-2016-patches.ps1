name: Test 2016 patches

on:
  workflow_dispatch:

jobs:
  test:
    runs-on: windows-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Run test script
        shell: pwsh
        run: .\.github\scripts\test-2016-patches.ps1
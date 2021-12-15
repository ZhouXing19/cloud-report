package cmd

func analysze_single() {
	cpu := newPerCloudAnalyzer(newCoremarkAnalyzer)
	defer cpu.Close()

	net := newPerCloudAnalyzer(newNetAnalyzer)
	defer net.Close()

	fio := newPerCloudAnalyzer(newFioAnalyzer)
	defer fio.Close()

	tpcc := newPerCloudAnalyzer(newTPCCAnalyzer)
	defer tpcc.Close()

}

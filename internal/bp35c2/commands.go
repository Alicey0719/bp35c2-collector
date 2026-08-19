package bp35c2

// Command codes used in this project. Only the ones we actually send
// are defined here; extend as needed.
const (
	// Requests (host → module)
	CmdInitialSettings    uint16 = 0x005F
	CmdBRouteAuthSet      uint16 = 0x0054
	CmdActiveScan         uint16 = 0x0051
	CmdBRouteStart        uint16 = 0x0053
	CmdBRouteStop         uint16 = 0x0058
	CmdUDPPortOpen        uint16 = 0x0005
	CmdUDPPortClose       uint16 = 0x0006
	CmdBRoutePANAStart    uint16 = 0x0056
	CmdBRoutePANAStop     uint16 = 0x0057
	CmdBRoutePANAReAuth   uint16 = 0x00D2
	CmdUDPSend            uint16 = 0x0008
	CmdHardReset          uint16 = 0x00D9
	CmdVersionGet         uint16 = 0x006B

	// Successful response codes = request | 0x2000
	RespInitialSettings   uint16 = 0x205F
	RespBRouteAuthSet     uint16 = 0x2054
	RespActiveScan        uint16 = 0x2051
	RespBRouteStart       uint16 = 0x2053
	RespBRouteStop        uint16 = 0x2058
	RespUDPPortOpen       uint16 = 0x2005
	RespUDPPortClose      uint16 = 0x2006
	RespBRoutePANAStart   uint16 = 0x2056
	RespBRoutePANAStop    uint16 = 0x2057
	RespBRoutePANAReAuth  uint16 = 0x20D2
	RespUDPSend           uint16 = 0x2008
	RespVersionGet        uint16 = 0x206B

	// Notifications (module-initiated)
	NotifyBoot           uint16 = 0x6019
	NotifyUDPReceive     uint16 = 0x6018
	NotifyPANAResult     uint16 = 0x6028
	NotifyLinkStateChg   uint16 = 0x601A
	NotifyActiveScanCh   uint16 = 0x4051

	// Special: header CS invalid / unknown command
	RespHeaderCSInvalid uint16 = 0x2FFF
	RespUnknownCmd      uint16 = 0xFFFF
)

// isResponse reports whether cmd is a response to a request (i.e.
// upper nibble 0x2XXX), as opposed to an asynchronous notification.
func isResponse(cmd uint16) bool {
	return cmd&0xF000 == 0x2000
}

package broute

// State is the connection lifecycle state.
type State int32

const (
	StateDisconnected State = iota
	StateInitializing
	StateScanning
	StateJoining
	StateConnected
	StateReconnecting
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateInitializing:
		return "initializing"
	case StateScanning:
		return "scanning"
	case StateJoining:
		return "joining"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	}
	return "unknown"
}

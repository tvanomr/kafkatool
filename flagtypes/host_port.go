package flagtypes

import (
	"fmt"
	"net"
	"strconv"
)

type HostPort struct {
	Host string
	Port int16
}

func (h *HostPort) String() string {
	return net.JoinHostPort(h.Host, strconv.FormatInt(int64(h.Port), 10))
}

//performs parse and host lookup

func (h *HostPort) Set(value string) error {
	ip := net.ParseIP(value)
	if ip != nil {
		h.Host = ip.String() //leave port as is allowing for defaults
		return nil
	}
	_, err := net.LookupHost(value)
	if err == nil {
		h.Host = value
		return nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("invalid host-port combination, %w", err)
	}
	_, err = net.LookupHost(host)
	if err != nil {
		return fmt.Errorf("host lookup failed for host %s: %w", host, err)
	}
	portNum, err := strconv.ParseInt(port, 10, 16)
	if err != nil {
		return fmt.Errorf("port %s is invalid: %w", port, err)
	}
	h.Host = host
	h.Port = int16(portNum)
	return nil
}

func (h *HostPort) Type() string {
	return "host[:port]"
}

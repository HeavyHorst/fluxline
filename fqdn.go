package fluxline

import (
	"net"
	"os"
	"strings"
)

func getFQDN() string {
	hostname, _ := os.Hostname()

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return hostname
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			hosts, err := net.LookupAddr(ipv4.String())
			if err != nil || len(hosts) == 0 {
				return hostname
			}
			fqdn := hosts[0]
			return strings.TrimSuffix(fqdn, ".") // return fqdn without trailing dot
		}
	}
	return hostname
}



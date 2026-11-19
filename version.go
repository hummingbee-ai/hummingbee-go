package hummingbee

import "runtime/debug"

const modulePath = "github.com/hummingbee-ai/hummingbee-go"

// userAgent names the module version an importer built with, read from the
// build info so there is no version constant to forget at release.
var userAgent = "hummingbee-go/" + moduleVersion()

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	for _, dep := range info.Deps {
		if dep.Path != modulePath {
			continue
		}
		if dep.Replace != nil && dep.Replace.Version != "" {
			return dep.Replace.Version
		}
		if dep.Version != "" {
			return dep.Version
		}
	}
	return "devel"
}

module github.com/pendo-io/mapreduce

go 1.12

require google.golang.org/appengine v1.6.6

require (
	github.com/cenkalti/backoff v0.0.0-20140713073939-f402310eb716
	github.com/pendo-io/appwrap v0.0.0-20200721181629-25611dfaa51c
	github.com/stretchr/testify v1.4.0
	golang.org/x/net v0.0.0-20200501053045-e0ff5e5a1de5
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20150330131248-af5907e7f8ac

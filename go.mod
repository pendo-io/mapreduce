module github.com/pendo-io/mapreduce

go 1.12

require google.golang.org/appengine v1.6.7

require (
	github.com/cenkalti/backoff v0.0.0-20140713073939-f402310eb716
	github.com/pendo-io/appwrap v0.0.0-20210722193206-fe0768a92620
	github.com/stretchr/testify v1.6.1
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20180220194651-af5907e7f8ac

package app

// Test-only exports of unexported App hooks, so the external test package
// (app_test) can reach them without widening the production API.  Every
// identifier here is referenced only from _test.go files.

// WithClock exposes withClock to the app_test package.
var WithClock = withClock

// WithSleep exposes withSleep to the app_test package.
var WithSleep = withSleep

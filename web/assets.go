package web

import "embed"

// Files contains the dashboard template and browser assets.
//
//go:embed templates/dashboard.html assets/app.css assets/app.js
var Files embed.FS

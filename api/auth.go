package api

import (
	"net/http"
)

type handler func(w http.ResponseWriter, r *http.Request)

func BasicAuth(pass handler) handler {

	return func(w http.ResponseWriter, r *http.Request) {
		//get Username from Cookie
		//r.Cookie("usernane")
		pass(w, r)
	}
}

func Validate(username, password string) bool {
	if username == "username" && password == "password" {
		return true
	}
	return false
}

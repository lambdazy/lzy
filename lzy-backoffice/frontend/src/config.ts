export function BACKEND_HOST(){
    if (window.location.protocol == "http"){
        return "http://" + window.location.hostname + ":8080"
    }
    return "https://" + window.location.hostname + ":8443"
}
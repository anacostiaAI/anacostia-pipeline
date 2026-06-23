var scriptTag = document.querySelector('script[src="static/js/third_party/settings.js"]');
const anacostia_prefix = scriptTag.getAttribute('anacostia-prefix');

document.addEventListener('htmx:configRequest', function(evt) {
    if (evt.detail.path.startsWith("/")) {
        evt.detail.path = anacostia_prefix + evt.detail.path;
    }
});
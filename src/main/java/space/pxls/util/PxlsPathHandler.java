package space.pxls.util;

import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;

public class PxlsPathHandler extends PathHandler {

    /**
     * Ungated Prefix path which is a CATCH ALL route for all requests STARTING with the route string defined in the "path" variable
     * @param path the route PREFIX path (ex. /ws)
     * @param handler the next request that comes after the called upon route
     */
    public synchronized PxlsPathHandler addPrefixPath(String path, HttpHandler handler) {
        return (PxlsPathHandler) super.addPrefixPath(path, handler);
    }

    /**
     * Gated Prefix path which is a CATCH ALL route for all requests STARTING with the route string defined in the "path" variable
     * @param path the route PREFIXpath (ex. /ws)
     * @param node defines the permission that the user has to have (permssions distributed via manual db+roles.conf)
     * @param handler the next request that comes after authorization was successful
     * @return
     */
    public synchronized PxlsPathHandler addPermGatedPrefixPath(String path, String node, HttpHandler handler) {
        return (PxlsPathHandler) super.addPrefixPath(path, new HttpPermissionGate(node, handler));
    }

    /**
     * Ungated Exact path which only accepts requests which are strictly the same as the route string defined in the "path" variable
     * @param path the route path (ex. /ws)
     * @param handler the next request that comes after the called upon route
     */
    public synchronized PxlsPathHandler addExactPath(String path, HttpHandler handler) {
        return (PxlsPathHandler) super.addExactPath(path, handler);
    }

    /**
     * Gated Exact path which only accepts requests which are strictly the same as the route string defined in the "path" variable
     * @param path the route path (ex. /ws)
     * @param node defines the permission that the user has to have (permssions distributed via manual db+roles.conf)
     * @param handler the next request that comes after authorization was successful
     * @return
     */
    public synchronized PxlsPathHandler addPermGatedExactPath(String path, String node, HttpHandler handler) {
        return (PxlsPathHandler) super.addExactPath(path, new HttpPermissionGate(node, handler));
    }
}

package space.pxls.util;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;
import space.pxls.user.Role;
import space.pxls.user.User;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HttpPermissionGate implements HttpHandler {
    String permission;
    HttpHandler next;

    /**
     * 
     * @param node The permission needed for the request
     * @param next The request that should be handled after verifying presence of correct permission
     */
    public HttpPermissionGate(String node, HttpHandler next) {
        this.permission = node;
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        User user = exchange.getAttachment(AuthReader.USER); //retrieve the user trying to make the request
        List<Role> roles = Role.getGuestRoles(); //retrieve the guest roles as the base list
        if (user != null) {
            roles = Stream.of(user.getRoles(), Role.getGuestRoles(), Role.getDefaultRoles()) //get the roles of the user and get the default roles a user has
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        // Sanity check--if the user has no roles, assume guest again.
        if (roles.isEmpty()) roles = Role.getGuestRoles();
        //If the user has the permission, handle the next request. This needs to be after the sanity check since guests also have *some* perms.
        if (roles.stream().anyMatch(role -> role.hasPermission(permission))) {
            next.handleRequest(exchange);
            return;
        }
        //if user does not have the correct permissions, return code 403
        exchange.setStatusCode(StatusCodes.FORBIDDEN);
        //end the exchange
        exchange.endExchange();
    }
}

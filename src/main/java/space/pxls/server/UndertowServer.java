package space.pxls.server;

import com.google.gson.JsonObject;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.server.handlers.DisableCacheHandler;
import io.undertow.server.handlers.form.EagerFormParsingHandler;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.FileResourceManager;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import space.pxls.App;
import space.pxls.server.packets.chat.*;
import space.pxls.server.packets.socket.*;
import space.pxls.tasks.UserAuthedTask;
import space.pxls.user.User;
import space.pxls.util.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

public class UndertowServer {
    private int port;
    private PacketHandler socketHandler;
    private WebHandler webHandler;
    private ConcurrentHashMap<Integer, User> authedUsers = new ConcurrentHashMap<Integer, User>();

    private Set<PxlsWebSocketConnection> connections;
    private Undertow server;

    private ExecutorService userTaskExecutor = Executors.newFixedThreadPool(4);

    public UndertowServer(int port) {
        this.port = port;

        webHandler = new WebHandler();
        socketHandler = new PacketHandler(this);
        connections = ConcurrentHashMap.newKeySet();
    }

    public void start() {
        var pathHandler = new PxlsPathHandler()
                .addPermGatedExactPath("/ws", "board.socket", Handlers.websocket(this::webSocketHandler)) // /ws route for the websocket
                .addPermGatedPrefixPath("/ws", "board.socket", Handlers.websocket(this::webSocketHandler)) // /ws? route for the websocket
                .addPermGatedPrefixPath("/info", "board.info", new DisableCacheHandler(webHandler::info)) //retrieves the following information: canvas-code, canvas-width, canvas-height, canvas-palette, cooldown-info, auth services, chatBannerText, snipmode, 7TV emote set, terms+conditions, chatratelimitmessage, chatlinkedminimumpixelcount, chatlinksendtostaff, chatdefaultexternallinkpopup
                .addPermGatedPrefixPath("/boarddata", "board.data", new DisableCacheHandler(webHandler::data)) //Retrieves the board.dat file from the server
                .addPermGatedPrefixPath("/heatmap", "board.data", new DisableCacheHandler(webHandler::heatmap)) //Retrieves the heatmap.dat file from the server
                .addPermGatedPrefixPath("/virginmap", "board.data", new DisableCacheHandler(webHandler::virginmap)) //Retrieves the virginmap.dat from the server
                .addPermGatedPrefixPath("/placemap", "board.data", new DisableCacheHandler(webHandler::placemap)) //Retrieves the placemap.dat from the server
                .addPermGatedPrefixPath("/initialboarddata", "board.data", webHandler::initialdata) //Retrieves the default_board.dat from the server
                .addPermGatedPrefixPath("/auth", "user.auth", new RateLimitingHandler(webHandler::auth, "http:auth", (int) App.getConfig().getDuration("server.limits.auth.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.auth.count"))) //Base authentication route for the auth, this is the route OAuth services use as callback. each OAuth provider has their own subpath. ex. /auth/discord or /auth/twitch
                .addPermGatedPrefixPath("/signin", "user.auth", webHandler::signIn) //Base signin path for sigining in a user. After authentication by OAuth provider, OAuth provider calls back to /auth/<OAuthprovidername> each OAuth provider has their own subpath. ex. /signin/discord 
                .addPermGatedPrefixPath("/signup", "user.auth", new RateLimitingHandler(webHandler::signUp, "http:signUp", (int) App.getConfig().getDuration("server.limits.signup.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.signup.count"))) //Base signup route for when a user does not have an account yet. after OAuth provider has verified the user, they callback to /auth/<OAuthprovidername>. each OAuth provider has their own subpath here. ex. /signup/discord or /signup/twitch
                .addPermGatedPrefixPath("/logout", "user.auth", webHandler::logout) //Route for logging the user out (yeets their pxls-token cookie)
                .addPermGatedPrefixPath("/lookup", "board.lookup", new RateLimitingHandler(webHandler::lookup, "http:lookup", (int) App.getConfig().getDuration("server.limits.lookup.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.lookup.count"))) //The route that is called when a user performs a pixel lookup on the canvas by shift-leftclicking. takes parameters in the form of ?x=&y=
                .addPermGatedPrefixPath("/report", "board.report", webHandler::report) //the route called when reporting a pixel 
                .addPermGatedPrefixPath("/reportChat", "chat.report", webHandler::chatReport) //the route called when reporting a user in chat
                .addPermGatedPrefixPath("/whoami", "user.auth", webHandler::whoami) // this route returns username and userid
                .addPermGatedPrefixPath("/users", "user.online", webHandler::users) // this route returns the amount of users online
                .addPermGatedPrefixPath("/chat/history", "chat.history", new RateLimitingHandler(new DisableCacheHandler(webHandler::chatHistory), "http:chatHistory", (int) App.getConfig().getDuration("server.limits.chatHistory.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.chatHistory.count"))) //returns full chat history in JSON format
                .addPermGatedPrefixPath("/chat/setColor", "user.chatColorChange", new RateLimitingHandler(webHandler::chatColorChange, "http:chatColorChange", (int) App.getConfig().getDuration("server.limits.chatColorChange.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.chatColorChange.count"))) //Route to set the chatcolor of the user
                .addPermGatedPrefixPath("/setDiscordName", "user.discordNameChange", new RateLimitingHandler(webHandler::discordNameChange, "http:discordName", (int) App.getConfig().getDuration("server.limits.discordNameChange.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.discordNameChange.count"))) //Route to change the discordname linked to the account
                .addPermGatedPrefixPath("/admin", "user.admin", Handlers.resource(new ClassPathResourceManager(App.class.getClassLoader(), "public/admin/")).setCacheTime(10)) //no clue what this should do, but it is the base admin route. probably caches something or checks if the user has admin perms
                .addPermGatedPrefixPath("/admin/ban", "user.ban", webHandler::ban) //The route called when banning a user for 24h from the canvas
                .addPermGatedPrefixPath("/admin/unban", "user.unban", webHandler::unban) //The route called when unbanning a user from the canvas
                .addPermGatedPrefixPath("/admin/permaban", "user.permaban", webHandler::permaban) //The route called when banning a user permanently from the canvas
                .addPermGatedPrefixPath("/admin/shadowban", "user.shadowban", webHandler::shadowban) //The route called when shadowbanning a user on the canvas
                .addPermGatedPrefixPath("/admin/chatban", "chat.ban", webHandler::chatban) //The route called when banning a user from chat
                .addPermGatedPrefixPath("/admin/check", "board.check", webHandler::check) //The route called when opening the "Check" panel of and admin with extra admin options and information
                .addPermGatedPrefixPath("/admin/delete", "chat.delete", webHandler::deleteChatMessage) //The route called when deleting a message from the db
                .addPermGatedPrefixPath("/admin/chatPurge", "chat.purge", webHandler::chatPurge) //The route called when purging the chat
                .addPermGatedPrefixPath("/execNameChange", "user.namechange", webHandler::execNameChange) //The route for NORMAL NONADMIN USERS when they are changing their name
                .addPermGatedPrefixPath("/admin/flagNameChange", "user.namechange.flag", webHandler::flagNameChange) //The route called when requesting a user to change their name
                .addPermGatedPrefixPath("/admin/forceNameChange", "user.namechange.force", webHandler::forceNameChange) //The route called when an ADMIN USER renames a user
                .addPermGatedPrefixPath("/admin/faction/edit", "faction.edit.other", new JsonReader(webHandler::adminEditFaction)) //Route to call when an admin is making changes to a faction
                .addPermGatedPrefixPath("/admin/faction/delete", "faction.delete.other", new JsonReader(webHandler::adminDeleteFaction)) //Route to call when an admin deletes a faction
                .addPermGatedPrefixPath("/admin/setFactionBlocked", "faction.setblocked", new AllowedMethodsHandler(webHandler::setFactionBlocked, Methods.POST)) //Route to call when an admin restricts a faction
                .addPermGatedPrefixPath("/createNotification", "notification.create", webHandler::createNotification) //Route to call when creating a board notification
                .addPermGatedPrefixPath("/sendNotificationToDiscord", "notification.discord", webHandler::sendNotificationToDiscord) //Route to call when sending a notification via the discord webhook
                .addPermGatedPrefixPath("/setNotificationExpired", "notification.expired", webHandler::setNotificationExpired) //Route to call when changing the expired date of a board notification
                .addPermGatedPrefixPath("/notifications", "notification.list", webHandler::notificationsList) //Returns the list with current active notifications
                .addPermGatedPrefixPath("/console", "management.console", new AllowedMethodsHandler(webHandler::webConsole, Methods.POST)) //why is there a webconsole endpoint????!
                .addPermGatedPrefixPath("/api/v1/profile", "user.profile", new AllowedMethodsHandler(webHandler::profile, Methods.GET)) //retrieves profile. why tf does this one have an /api/ route?
                .addExactPath("/factions", new AllowedMethodsHandler(webHandler::getRequestingUserFactions, Methods.GET)); //get the available factions in JSON format
        if (new File(App.getStorageDir().resolve("emoji").toString()).exists()) {
            pathHandler.addPrefixPath("/emoji", Handlers.resource(new FileResourceManager(new File(App.getStorageDir().resolve("emoji").toString()))).setCacheTime(604800)); //if there is a folder with emoji's, expose the emoji route so the emotes can be use in chat
        }
        PxlsRoutingHandler routingHandler = PxlsHandlers.routing()
            .getPermGated("/factions/{fid}", "faction.data", new JsonReader(new RateLimitingHandler(webHandler::manageFactions, "http:manageFactions", (int) App.getConfig().getDuration("server.limits.manageFactions.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.manageFactions.count"), App.getConfig().getBoolean("server.limits.manageFactions.global"))))
            .postPermGated("/factions", "faction.create", new JsonReader(new RateLimitingHandler(webHandler::manageFactions, "http:manageFactions", (int) App.getConfig().getDuration("server.limits.manageFactions.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.manageFactions.count"), App.getConfig().getBoolean("server.limits.manageFactions.global"))))
            .putPermGated("/factions/{fid}", "faction.edit", new JsonReader(new RateLimitingHandler(webHandler::manageFactions, "http:manageFactions", (int) App.getConfig().getDuration("server.limits.manageFactions.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.manageFactions.count"), App.getConfig().getBoolean("server.limits.manageFactions.global"))))
            .deletePermGated("/factions/{fid}", "faction.delete", new JsonReader(new RateLimitingHandler(webHandler::manageFactions, "http:manageFactions", (int) App.getConfig().getDuration("server.limits.manageFactions.time", TimeUnit.SECONDS), App.getConfig().getInt("server.limits.manageFactions.count"), App.getConfig().getBoolean("server.limits.manageFactions.global"))))
            .setFallbackHandler(pathHandler);
        //EncodingHandler encoder = new EncodingHandler(mainHandler, new ContentEncodingRepository().addEncodingHandler("gzip", new GzipEncodingProvider(), 50, Predicates.parse("max-content-size(1024)")));
        server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setIoThreads(32)
                .setWorkerThreads(128)
                .setHandler(new IPReader(new AuthReader(new EagerFormParsingHandler().setNext(routingHandler)))).build();
        server.start();
    }

    private void webSocketHandler(WebSocketHttpExchange exchange, WebSocketChannel channel) {
        User user = exchange.getAttachment(AuthReader.USER);
        String ip = exchange.getAttachment(IPReader.IP);

        socketHandler.connect(channel, user);

        PxlsWebSocketConnection con = new PxlsWebSocketConnection(channel, user);
        connections.add(con);

        if (user != null) {
            user.getConnections().add(channel);

            // aaaaaaand update the useragent
            List<String> agentAr = exchange.getRequestHeaders().get(Headers.USER_AGENT.toString());
            String agent = "";
            if (agentAr != null) {
                agent = agentAr.get(0);
            }
            if (agent == null) {
                agent = "";
            }
            user.setUserAgent(agent);

            userTaskExecutor.submit(new UserAuthedTask(channel, user, ip)); //ip at this point should have gone through all the checks to extract an actual IP from behind a reverse proxy
        }

        channel.getReceiveSetter().set(new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) throws IOException {
                super.onFullTextMessage(channel, message);

                String data = message.getData();

                JsonObject jsonObj = App.getGson().fromJson(data, JsonObject.class);
                String type = jsonObj.get("type").getAsString();

                Object obj = null;
                if (type.equals("pixel")) obj = App.getGson().fromJson(jsonObj, ClientPlace.class);
                if (type.equals("undo")) obj = App.getGson().fromJson(jsonObj, ClientUndo.class);
                if (type.equals("captcha")) obj = App.getGson().fromJson(jsonObj, ClientCaptcha.class);
                if (type.equals("admin_placement_overrides")) obj = App.getGson().fromJson(jsonObj, ClientAdminPlacementOverrides.class);
                if (type.equals("admin_message")) obj = App.getGson().fromJson(jsonObj, ClientAdminMessage.class);
                if (type.equals("shadowbanme")) obj = App.getGson().fromJson(jsonObj, ClientShadowBanMe.class);
                if (type.equals("banme")) obj = App.getGson().fromJson(jsonObj, ClientBanMe.class);
                if (type.equalsIgnoreCase("ChatHistory")) obj = App.getGson().fromJson(jsonObj, ClientChatHistory.class);
                if (type.equalsIgnoreCase("ChatbanState")) obj = App.getGson().fromJson(jsonObj, ClientChatbanState.class);
                if (type.equalsIgnoreCase("ChatMessage")) obj = App.getGson().fromJson(jsonObj, ClientChatMessage.class);
                if (type.equalsIgnoreCase("ChatLookup")) obj = App.getGson().fromJson(jsonObj, ClientChatLookup.class);

                // old thing, will auto-shadowban
                if (type.equals("place")) obj = App.getGson().fromJson(jsonObj, ClientPlace.class);

                // lol
                if (type.equals("placepixel")) obj = App.getGson().fromJson(jsonObj, ClientBanMe.class);

                if (obj != null) {
                    socketHandler.accept(channel, user, obj, ip);
                }
            }
        });
        channel.getCloseSetter().set(c -> {
            connections.remove(con);

            if (user != null) {
                user.getConnections().remove(channel);
            }

            socketHandler.disconnect(channel, user);
        });
        channel.resumeReceives();
    }

    public Set<PxlsWebSocketConnection> getConnections() {
        return connections;
    }

    public void broadcast(Object obj) {
        String json = App.getGson().toJson(obj);
        if (connections != null) {
            for (PxlsWebSocketConnection channel : connections) {
                sendRaw(channel, json);
            }
        }
    }

    public void broadcastRaw(String raw) {
        if (connections != null) {
            connections.forEach(channel -> sendRaw(channel, raw));
        }
    }

    public void broadcastNoShadow(Object obj) {
        broadcastToUserPredicate(obj, user -> !user.isShadowBanned());
    }

    private Predicate<User> userCanReceiveStaffBroadcasts = user -> user.hasPermission("user.receivestaffbroadcasts");

    public void broadcastToStaff(Object obj) {
        broadcastToUserPredicate(obj, userCanReceiveStaffBroadcasts);
    }

    public void broadcastToUserPredicate(Object obj, Predicate<User> predicate) {
        String json = App.getGson().toJson(obj);
        getAuthedUsers()
                .values()
                .stream()
                .filter(predicate)
                .forEach(user -> user.getConnections()
                        .forEach(con -> WebSockets.sendText(json, con, null))
                );
    }

    public void broadcastPredicate(Object obj, Predicate<PxlsWebSocketConnection> predicate) {
        String json = App.getGson().toJson(obj);
        connections.parallelStream()
                .filter(predicate)
                .forEach(con -> WebSockets.sendText(json, con.getChannel(), null));
    }

    public void broadcastSeparateForStaff(Object nonStaffObj, Object staffObj) {
        broadcastPredicateSeparateForStaff(nonStaffObj, staffObj, con -> true);
    }

    public void broadcastPredicateSeparateForStaff(Object nonStaffObj, Object staffObj, Predicate<PxlsWebSocketConnection> predicate) {
        String nonStaffJSON = nonStaffObj != null ? App.getGson().toJson(nonStaffObj) : null;
        String staffJSON = staffObj != null ? App.getGson().toJson(staffObj) : null;
        broadcastMapped(con -> {
            if (predicate.test(con)) {
                boolean sendStaffObject = con.getUser().isPresent() && userCanReceiveStaffBroadcasts.test(con.getUser().get());
                return sendStaffObject ? staffJSON : nonStaffJSON;
            } else {
                return null;
            }
        });
    }

    public void broadcastMapped(Function<PxlsWebSocketConnection, String> mapper) {
        connections.parallelStream()
                .forEach(con -> {
                    String json = mapper.apply(con);
                    if (json != null) {
                        WebSockets.sendText(json, con.getChannel(), null);
                    }
                });
    }

    public void send(WebSocketChannel channel, Object obj) {
        sendRaw(channel, App.getGson().toJson(obj));
    }

    public void send(User user, Object obj) {
        sendRaw(user, App.getGson().toJson(obj));
    }

    public void sendRaw(User user, String raw) {
        user.getConnections().forEach(channel -> sendRaw(channel, raw));
    }

    private void sendRaw(PxlsWebSocketConnection channel, String str) {
        WebSockets.sendText(str, channel.getChannel(), null);
    }

    private void sendRaw(WebSocketChannel channel, String str) {
        WebSockets.sendText(str, channel, null);
    }

    public PacketHandler getPacketHandler() {
        return socketHandler;
    }

    public void addAuthedUser(User user) {
        if (!authedUsers.containsKey(user.getId()) && !user.isBanned() && !user.isShadowBanned()) {
            authedUsers.put(user.getId(), user);
        }
    }

    public void removeAuthedUser(User user) {
        authedUsers.remove(user.getId());
    }

    public ConcurrentHashMap<Integer, User> getAuthedUsers() {
        return this.authedUsers;
    }

    public int getNonIdledUsersCount() {
        int nonIdles = 0;
        for (User value : App.getServer().getAuthedUsers().values()) {
            if (!value.isIdled()) ++nonIdles;
        }
        return nonIdles;
    }

    public Undertow getServer() {
        return server;
    }

    public WebHandler getWebHandler() {
        return webHandler;
    }
}

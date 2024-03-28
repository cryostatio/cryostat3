/*
 * Copyright The Cryostat Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cryostat.ws;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
@ServerEndpoint("/api/notifications")
public class MessagingServer {

    private static final String CLIENT_ACTIVITY_CATEGORY = "WsClientActivity";

    @Inject Logger logger;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Notification> msgQ;
    private final Set<Session> sessions = new CopyOnWriteArraySet<>();
    private final ObjectMapper mapper = new ObjectMapper();

    MessagingServer(@ConfigProperty(name = "cryostat.messaging.queue.size") int capacity) {
        this.msgQ = new ArrayBlockingQueue<>(capacity);
    }

    @OnOpen
    public void onOpen(Session session) {
        logger.debugv("Adding session {0}", session.getId());
        sessions.add(session);
        broadcast(new Notification(CLIENT_ACTIVITY_CATEGORY, Map.of(session.getId(), "connected")));
    }

    @OnClose
    public void onClose(Session session) {
        logger.debugv("Removing session {0}", session.getId());
        sessions.remove(session);
        broadcast(
                new Notification(
                        CLIENT_ACTIVITY_CATEGORY, Map.of(session.getId(), "disconnected")));
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        logger.error("Session error", throwable);
        try {
            logger.errorv("Closing session {0}", session.getId());
            session.close();
        } catch (IOException ioe) {
            logger.error("Unable to close session", ioe);
        }
        broadcast(
                new Notification(
                        CLIENT_ACTIVITY_CATEGORY, Map.of(session.getId(), "disconnected")));
    }

    void start(@Observes StartupEvent evt) {
        logger.infov("Starting {0} executor", getClass().getName());
        executor.execute(
                () -> {
                    while (!executor.isShutdown()) {
                        try {
                            var notification = msgQ.take();
                            var map =
                                    Map.of(
                                            "meta",
                                            Map.of("category", notification.category()),
                                            "message",
                                            notification.message());
                            try {
                                var json = mapper.writeValueAsString(map);
                                logger.infov("Broadcasting: {0}", map);
                                logger.debugv("Broadcasting: {0}", json);
                                sessions.forEach(s -> s.getAsyncRemote().sendText(json));
                            } catch (JsonProcessingException e) {
                                logger.error("Unable to serialize message to JSON", e);
                            }
                        } catch (InterruptedException ie) {
                            logger.warn(ie);
                            break;
                        }
                    }
                });
    }

    void shutdown(@Observes ShutdownEvent evt) {
        logger.infov("Shutting down {0} executor", getClass().getName());
        executor.shutdown();
        msgQ.clear();
    }

    @OnMessage
    public void onMessage(Session session, String message) {
        logger.debugv("{0} message: \"{1}\"", session.getId(), message);
    }

    @ConsumeEvent
    void broadcast(Notification notification) {
        msgQ.offer(notification);
    }
}

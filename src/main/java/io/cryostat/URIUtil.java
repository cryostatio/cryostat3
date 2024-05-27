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
package io.cryostat;

import java.net.URI;
import java.net.URISyntaxException;

import javax.management.remote.JMXServiceURL;

import org.openjdk.jmc.rjmx.common.ConnectionToolkit;

import io.cryostat.util.URIRange;

public class URIUtil {

    private URIRange uriRange;

    public URIUtil(URIRange uriRange) {
        this.uriRange = uriRange;
    }

    public URI createAbsolute(String uri) throws URISyntaxException, RelativeURIException {
        URI u = new URI(uri);
        if (!u.isAbsolute()) {
            throw new RelativeURIException(u);
        }
        return u;
    }

    public URI convert(JMXServiceURL serviceUrl) throws URISyntaxException {
        return new URI(serviceUrl.toString());
    }

    public URI getRmiTarget(JMXServiceURL serviceUrl) throws URISyntaxException {
        String rmiPart = "/jndi/rmi://";
        String pathPart = serviceUrl.getURLPath();
        if (!pathPart.startsWith(rmiPart)) {
            throw new IllegalArgumentException(serviceUrl.getURLPath());
        }
        return new URI(pathPart.substring("/jndi/".length(), pathPart.length()));
    }

    public boolean validateUri(URI uri) {
        return uriRange.validate(uri.getHost());
    }

    public boolean validateJmxServiceURL(JMXServiceURL jmxUrl) {
        String hostname = ConnectionToolkit.getHostName(jmxUrl);
        return uriRange.validate(hostname);
    }

    public static class RelativeURIException extends URISyntaxException {
        public RelativeURIException(URI u) {
            super(u.toString(), "Not a valid absolute URI");
        }
    }
}

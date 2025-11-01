package org.noise_planet.noisemodelling.jdbc.utils;

import org.h2gis.postgis_jts.ConnectionWrapper;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection wrapper that keeps the H2GIS {@link ConnectionWrapper} behaviour while ensuring that calls to
 * {@link #unwrap(Class)} return the proxy itself when the {@code Connection} interface is requested. This prevents
 * downstream libraries from bypassing the wrapper and retrieving raw PostgreSQL objects (for example {@code PGgeometry})
 * that are not compatible with the H2GIS export stack.
 */
public class PostgisConnectionWrapper extends ConnectionWrapper {
    private final Connection delegate;

    public PostgisConnectionWrapper(Connection delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return true;
        }
        return delegate.isWrapperFor(iface);
    }
}

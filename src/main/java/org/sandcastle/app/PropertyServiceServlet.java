package main.java.org.sandcastle.app;

import javascalautils.Option;
import javascalautils.Try;
import javascalautils.Unit;
import org.dmonix.servlet.JSONServlet;
import org.dmonix.servlet.Request;
import org.dmonix.servlet.Response;
import org.dmonix.zookeeper.PropertiesStorage;
import org.dmonix.zookeeper.PropertiesStorageFactory;
import org.dmonix.zookeeper.PropertySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;
import java.util.List;
import java.util.Map;

import static javascalautils.OptionCompanion.Option;
import static javax.servlet.http.HttpServletResponse.*;

@WebServlet(name = "PropertyService", displayName = "ZooKeeper Properties over rest-api", description = "RESTful interface for managing properties stored in ZooKeeper", urlPatterns = {
        "/properties/*"}, loadOnStartup = 1, initParams = {
        @WebInitParam(name = "connectString", value = "localhost:2181"),
        @WebInitParam(name = "rootPath", value = "/")})
public final class PropertyServiceServlet extends JSONServlet {

    private static final Logger logger = LoggerFactory.getLogger(PropertyServiceServlet.class);

    /**
     * Unique serial version identifier
     */
    private static final long serialVersionUID = 1L;

    /**
     * factory to create access to ZooKeeper storage
     */
    private PropertiesStorageFactory propertiesStorageFactory;

    /**
     * Initializes servlet with opening zk connection from configuration
     *
     * @param config Indicates servlet configuration to be used during initialization.
     * @throws ServletException gets thrown if failed to initialize servlet successfully
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        logger.info("Starting PropertyServiceServlet");
        logger.info("connectString = {}", config.getInitParameter("connectString"));
        logger.info("rootPath = {}", config.getInitParameter("rootPath"));

        propertiesStorageFactory = PropertiesStorageFactory.apply(config.getInitParameter("connectString"));
        Option(config.getInitParameter("rootPath")).forEach(value -> propertiesStorageFactory.withRootPath(value));
    }

    /**
     * Manages storage of property sets.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Response put(Request req) {
        return req.getPathInfo().map(name -> {
            Try<PropertySet> propSet = props(name, req);
            // orNull will never happen as we installed a recover function
            return storeProperties(propSet)
                    .map(u -> EmptyResponse(SC_CREATED))
                    .recover(this::ErrorResponse)
                    .orNull();
        }).getOrElse(() -> ErrorResponse(SC_BAD_REQUEST, "Missing property-set name"));
    }

    /**
     * Posts property-set change.
     *
     * @param req Indicates servlet request containing new properties.
     */
    @Override
    protected Response post(Request req) {
        return req.getPathInfo().map(name -> {
            Try<PropertySet> toBeStored = props(name, req)
                    .flatMap(newProps -> getStoredProperties(name).map(storedProps -> {
                        PropertySet combinedProps = storedProps.getOrElse(() -> PropertySet.apply(name));
                        newProps.asMap().forEach(combinedProps::set);
                        return combinedProps;
                    }));
            // orNull will never happen as we installed a recover function
            return storeProperties(toBeStored)
                    .map(u -> EmptyResponse(SC_CREATED))
                    .recover(this::ErrorResponse)
                    .orNull();
        }).getOrElse(() -> ErrorResponse(SC_BAD_REQUEST, "Missing property-set name"));
    }

    /**
     * Manages both listing the names of all property sets and listing properties for an individual set.
     */
    @Override
    protected Try<Response> getWithTry(Request req) {
        String path = req.getPathInfo().getOrElse(() -> "");

        Try<Response> response;
        if (path.isEmpty()) {
            logger.debug("Requesting all property-set names");
            Try<List<String>> result = createStorage().flatMap(PropertiesStorage::propertySets);
            response = result.map(this::ObjectResponse);
        } else {
            logger.debug("Requesting data for property [{}]", path);
            response = getStoredProperties(path).map(this::PropertySetResponse);
        }
        return response;
    }

    /**
     * Manages delete of a specified property set.
     */
    @Override
    protected Response delete(Request req) {
        return req.getPathInfo().map(name -> {
            logger.debug("Deleting property set [{}]", name);
            Try<Unit> result = createStorage().flatMap(storage -> storage.delete(name));
            return result.map(r -> EmptyResponse(SC_OK)).recover(this::ErrorResponse).orNull();
        }).getOrElse(() -> ErrorResponse(SC_BAD_REQUEST, "Missing property-set name"));
    }

    /**
     * (non-JavaDoc)
     */
    private Try<Unit> storeProperties(Try<PropertySet> props) {
        return props.flatMap(set -> createStorage().flatMap(storage -> storage.store(set)));
    }

    /**
     * (non-JavaDoc)
     */
    private Try<Option<PropertySet>> getStoredProperties(String name) {
        return createStorage().flatMap(storage -> storage.get(name));
    }

    /**
     * (non-JavaDoc)
     *
     * @return Created property-set storage
     */
    private Try<PropertiesStorage> createStorage() {
        return propertiesStorageFactory.create().map(AutoCloseablePropertiesStorage::new);
    }

    @SuppressWarnings("unchecked")
    private static Try<PropertySet> props(String name, Request req) {
        return req.fromJson(Map.class)
                .map(m -> (Map<String, String>) m)
                .map(map -> {
                    PropertySet set = PropertySet.apply(name);
                    map.forEach(set::set);
                    logger.debug("Storing property [{}]", set);
                    return set;
                });
    }

    private Response PropertySetResponse(Option<PropertySet> propertySet) {
        return propertySet.map(p -> ObjectResponse(p.asMap()))
                .getOrElse(() -> ErrorResponse(SC_NOT_FOUND, "No such property set"));
    }

}

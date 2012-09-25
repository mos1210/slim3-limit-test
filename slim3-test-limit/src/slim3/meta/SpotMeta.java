package slim3.meta;

//@javax.annotation.Generated(value = { "slim3-gen", "@VERSION@" }, date = "2012-09-26 01:50:13")
/** */
public final class SpotMeta extends org.slim3.datastore.ModelMeta<slim3.model.Spot> {

    /** */
    public final org.slim3.datastore.CoreAttributeMeta<slim3.model.Spot, com.google.appengine.api.datastore.Key> key = new org.slim3.datastore.CoreAttributeMeta<slim3.model.Spot, com.google.appengine.api.datastore.Key>(this, "__key__", "key", com.google.appengine.api.datastore.Key.class);

    /** */
    public final org.slim3.datastore.CoreAttributeMeta<slim3.model.Spot, java.lang.Long> version = new org.slim3.datastore.CoreAttributeMeta<slim3.model.Spot, java.lang.Long>(this, "version", "version", java.lang.Long.class);

    private static final SpotMeta slim3_singleton = new SpotMeta();

    /**
     * @return the singleton
     */
    public static SpotMeta get() {
       return slim3_singleton;
    }

    /** */
    public SpotMeta() {
        super("Spot", slim3.model.Spot.class);
    }

    @Override
    public slim3.model.Spot entityToModel(com.google.appengine.api.datastore.Entity entity) {
        slim3.model.Spot model = new slim3.model.Spot();
        model.setKey(entity.getKey());
        model.setVersion((java.lang.Long) entity.getProperty("version"));
        return model;
    }

    @Override
    public com.google.appengine.api.datastore.Entity modelToEntity(java.lang.Object model) {
        slim3.model.Spot m = (slim3.model.Spot) model;
        com.google.appengine.api.datastore.Entity entity = null;
        if (m.getKey() != null) {
            entity = new com.google.appengine.api.datastore.Entity(m.getKey());
        } else {
            entity = new com.google.appengine.api.datastore.Entity(kind);
        }
        entity.setProperty("version", m.getVersion());
        entity.setProperty("slim3.schemaVersion", 1);
        return entity;
    }

    @Override
    protected com.google.appengine.api.datastore.Key getKey(Object model) {
        slim3.model.Spot m = (slim3.model.Spot) model;
        return m.getKey();
    }

    @Override
    protected void setKey(Object model, com.google.appengine.api.datastore.Key key) {
        validateKey(key);
        slim3.model.Spot m = (slim3.model.Spot) model;
        m.setKey(key);
    }

    @Override
    protected long getVersion(Object model) {
        slim3.model.Spot m = (slim3.model.Spot) model;
        return m.getVersion() != null ? m.getVersion().longValue() : 0L;
    }

    @Override
    protected void assignKeyToModelRefIfNecessary(com.google.appengine.api.datastore.AsyncDatastoreService ds, java.lang.Object model) {
    }

    @Override
    protected void incrementVersion(Object model) {
        slim3.model.Spot m = (slim3.model.Spot) model;
        long version = m.getVersion() != null ? m.getVersion().longValue() : 0L;
        m.setVersion(Long.valueOf(version + 1L));
    }

    @Override
    protected void prePut(Object model) {
    }

    @Override
    protected void postGet(Object model) {
    }

    @Override
    public String getSchemaVersionName() {
        return "slim3.schemaVersion";
    }

    @Override
    public String getClassHierarchyListName() {
        return "slim3.classHierarchyList";
    }

    @Override
    protected boolean isCipherProperty(String propertyName) {
        return false;
    }

    @Override
    protected void modelToJson(org.slim3.datastore.json.JsonWriter writer, java.lang.Object model, int maxDepth, int currentDepth) {
        slim3.model.Spot m = (slim3.model.Spot) model;
        writer.beginObject();
        org.slim3.datastore.json.Default encoder0 = new org.slim3.datastore.json.Default();
        if(m.getKey() != null){
            writer.setNextPropertyName("key");
            encoder0.encode(writer, m.getKey());
        }
        if(m.getVersion() != null){
            writer.setNextPropertyName("version");
            encoder0.encode(writer, m.getVersion());
        }
        writer.endObject();
    }

    @Override
    protected slim3.model.Spot jsonToModel(org.slim3.datastore.json.JsonRootReader rootReader, int maxDepth, int currentDepth) {
        slim3.model.Spot m = new slim3.model.Spot();
        org.slim3.datastore.json.JsonReader reader = null;
        org.slim3.datastore.json.Default decoder0 = new org.slim3.datastore.json.Default();
        reader = rootReader.newObjectReader("key");
        m.setKey(decoder0.decode(reader, m.getKey()));
        reader = rootReader.newObjectReader("version");
        m.setVersion(decoder0.decode(reader, m.getVersion()));
        return m;
    }
}
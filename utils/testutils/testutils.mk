# testutils fragment for the root Makefile: the containers unit tests reach through utils/testutils/testhandler.
# Tests never start them: CI runs testutils.all.start, and a local test fails naming the target to run.

TESTUTILS_COMPOSE = $(COMPOSE) -f destination/iceberg/local-test/catalog/docker-compose.yml
TESTUTILS_SERVICES := minio iceberg-rest hive-metastore glue postgres iceberg-writer
# The driver stacks the unit tests share with the integration suites, started by olake.<driver>.start.
TESTUTILS_DRIVERS := postgres mysql mongodb mssql kafka

TESTUTILS_MC = docker exec --env MC_HOST_local=http://olake:olake-testhandler@127.0.0.1:9000 olake-catalog-minio mc
PROBE.testutils.minio = $(TESTUTILS_MC) ls local
POST_SETUP.testutils.minio = $(TESTUTILS_MC) mb --ignore-existing local/warehouse
PROBE.testutils.iceberg-rest = curl -sf http://127.0.0.1:18181/v1/config
# Probed inside the container: Docker's port proxy accepts host connections before the metastore listens.
PROBE.testutils.hive-metastore = docker exec olake-catalog-hive-metastore bash -c 'exec 3<>/dev/tcp/127.0.0.1/9083'
WAIT_RETRIES.testutils.hive-metastore := 60
PROBE.testutils.glue = curl -sf http://127.0.0.1:15000/moto-api/
# Over TCP inside the container: the image's first start serves only its socket while it creates the database.
PROBE.testutils.postgres = docker exec olake-catalog-postgres pg_isready --host 127.0.0.1 --username olake --dbname olake_testhandler_catalog
# Probed inside the container: Docker's port proxy accepts host connections before the JVM listens.
PROBE.testutils.iceberg-writer = docker exec olake-catalog-iceberg-writer bash -c 'exec 3<>/dev/tcp/127.0.0.1/50051'

define TESTUTILS_template
.PHONY: testutils.$(1).start
testutils.$(1).start:
	$$(TESTUTILS_COMPOSE) up -d $(1)
	@$$(call wait_ready,testutils.$(1))
	@$$(POST_SETUP.testutils.$(1))
endef
$(foreach s,$(TESTUTILS_SERVICES),$(eval $(call TESTUTILS_template,$(s))))

# Both catalogs keep their warehouse in MinIO's bucket.
testutils.iceberg-rest.start testutils.hive-metastore.start: testutils.minio.start
# The writer JVM opens the JDBC catalog and the MinIO warehouse when it starts, and runs the jar make builds.
testutils.iceberg-writer.start: testutils.minio.start testutils.postgres.start iceberg.jar

.PHONY: testutils.all.start testutils.all.stop
testutils.all.start: $(addprefix olake.,$(addsuffix .start,$(TESTUTILS_DRIVERS))) $(addprefix testutils.,$(addsuffix .start,$(TESTUTILS_SERVICES)))

# Leaves the driver stacks up: the integration suites share them, and olake.<driver>.stop stops each.
testutils.all.stop:
	$(TESTUTILS_COMPOSE) down --remove-orphans

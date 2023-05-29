import json
import os
import typing as t
from collections import defaultdict
from functools import update_wrapper

from . import typing as ft
from .scaffold import _endpoint_from_view_func
from .scaffold import _sentinel
from .scaffold import Scaffold
from .scaffold import setupmethod
@Trace
@Singleton
public class ElasticSearchDAO implements IndexDAO {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDAO.class);

	private static final String WORKFLOW_DOC_TYPE = "workflow";

	private static final String TASK_DOC_TYPE = "task";

	private static final String LOG_DOC_TYPE = "task_log";

	private static final String EVENT_DOC_TYPE = "event";

	private static final String MSG_DOC_TYPE = "message";

	private static final String className = ElasticSearchDAO.class.getSimpleName();

	private static final int RETRY_COUNT = 3;

	private final int archiveSearchBatchSize;

	private String indexName;

	private String logIndexName;

	private String logIndexPrefix;

	private ObjectMapper objectMapper;

	private Client elasticSearchClient;

	private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMWW");

	private final ExecutorService executorService;
	private final ExecutorService logExecutorService;

	static {
		SIMPLE_DATE_FORMAT.setTimeZone(GMT);
	}

	@Inject
	public ElasticSearchDAO(Client elasticSearchClient, Configuration config, ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		this.elasticSearchClient = elasticSearchClient;
		this.indexName = config.getProperty("workflow.elasticsearch.index.name", null);
		this.archiveSearchBatchSize = config.getIntProperty("workflow.elasticsearch.archive.search.batchSize", 5000);

		int corePoolSize = 4;
		int maximumPoolSize = config.getIntProperty("workflow.elasticsearch.async.dao.max.pool.size", 12);
		long keepAliveTime = 1L;
		int workerQueueSize = config.getIntProperty("workflow.elasticsearch.async.dao.worker.queue.size", 100);
		this.executorService = new ThreadPoolExecutor(corePoolSize,
			maximumPoolSize,
			keepAliveTime,
			TimeUnit.MINUTES,
			new LinkedBlockingQueue<>(workerQueueSize),
			(runnable, executor) -> {
				logger.warn("Request {} to async dao discarded in executor {}", runnable, executor);
				Monitors.recordDiscardedIndexingCount("indexQueue");
			});

		corePoolSize = 1;
		maximumPoolSize = 2;
		keepAliveTime = 30L;
		this.logExecutorService = new ThreadPoolExecutor(corePoolSize,
			maximumPoolSize,
			keepAliveTime,
			TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(workerQueueSize),
			(runnable, executor) -> {
				logger.warn("Request {} to async log dao discarded in executor {}", runnable, executor);
				Monitors.recordDiscardedIndexingCount("logQueue");
			});

		try {
			initIndex();
			updateIndexName(config);
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> updateIndexName(config), 0, 1, TimeUnit.HOURS);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@PreDestroy
	private void shutdown() {
		logger.info("Gracefully shutdown executor service");
		shutdownExecutorService(logExecutorService);
		shutdownExecutorService(executorService);
	}

	private void shutdownExecutorService(ExecutorService execService) {
		try {
			execService.shutdown();
			if (execService.awaitTermination(30, TimeUnit.SECONDS)) {
				logger.debug("tasks completed, shutting down");
			} else {
				logger.warn("forcing shutdown after waiting for 30 seconds");
				execService.shutdownNow();
			}
		} catch (InterruptedException ie) {
			logger.warn("shutdown interrupted, invoking shutdownNow");
			execService.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	private void updateIndexName(Configuration config) {
		this.logIndexPrefix = config.getProperty("workflow.elasticsearch.tasklog.index.name", "task_log");
		this.logIndexName = this.logIndexPrefix + "_" + SIMPLE_DATE_FORMAT.format(new Date());

		try {
			elasticSearchClient.admin().indices().prepareGetIndex().addIndices(logIndexName).execute().actionGet();
		} catch (IndexNotFoundException infe) {
			try {
				elasticSearchClient.admin().indices().prepareCreate(logIndexName).execute().actionGet();
			} catch (IndexAlreadyExistsException ignored) {

			} catch (Exception e) {
				logger.error("Error creating log index", e);
			}
		}
	}

	/**
	 * Initializes the index with required templates and mappings.
	 */
	private void initIndex() throws Exception {

		//0. Add the tasklog template
		GetIndexTemplatesResponse result = elasticSearchClient.admin().indices().prepareGetTemplates("tasklog_template").execute().actionGet();
		if (result.getIndexTemplates().isEmpty()) {
			logger.info("Creating the index template 'tasklog_template'");
			InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/template_tasklog.json");
			byte[] templateSource = IOUtils.toByteArray(stream);

			try {
				elasticSearchClient.admin().indices().preparePutTemplate("tasklog_template").setSource(templateSource).execute().actionGet();
			} catch (Exception e) {
				logger.error("Failed to init tasklog_template", e);
			}
		}

		//1. Create the required index
		try {
			elasticSearchClient.admin().indices().prepareGetIndex().addIndices(indexName).execute().actionGet();
		} catch (IndexNotFoundException infe) {
			try {
				elasticSearchClient.admin().indices().prepareCreate(indexName).execute().actionGet();
			} catch (IndexAlreadyExistsException ignored) {
			}
		}

		//2. Add Mappings for the workflow document type
		GetMappingsResponse getMappingsResponse = elasticSearchClient.admin().indices().prepareGetMappings(indexName).addTypes(WORKFLOW_DOC_TYPE).execute().actionGet();
		if (getMappingsResponse.mappings().isEmpty()) {
			logger.info("Adding the workflow type mappings");
			InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/mappings_docType_workflow.json");
			byte[] bytes = IOUtils.toByteArray(stream);
			String source = new String(bytes);
			try {
				elasticSearchClient.admin().indices().preparePutMapping(indexName).setType(WORKFLOW_DOC_TYPE).setSource(source).execute().actionGet();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		//3. Add Mappings for task document type
		getMappingsResponse = elasticSearchClient.admin().indices().prepareGetMappings(indexName).addTypes(TASK_DOC_TYPE).execute().actionGet();
		if (getMappingsResponse.mappings().isEmpty()) {
			logger.info("Adding the task type mappings");
			InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/mappings_docType_task.json");
			byte[] bytes = IOUtils.toByteArray(stream);
			String source = new String(bytes);
			try {
				elasticSearchClient.admin().indices().preparePutMapping(indexName).setType(TASK_DOC_TYPE).setSource(source).execute().actionGet();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	@Override
	public void setup() {
	}

	@Override
	public void indexWorkflow(Workflow workflow) {
		try {
			long startTime = Instant.now().toEpochMilli();
			String workflowId = workflow.getWorkflowId();
			WorkflowSummary summary = new WorkflowSummary(workflow);
			byte[] doc = objectMapper.writeValueAsBytes(summary);

			logger.debug("Indexing workflow document: {}", workflowId);
			UpdateRequest req = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, workflowId);
			req.doc(doc);
			req.upsert(doc);
			req.retryOnConflict(5);
			indexDocument(req, WORKFLOW_DOC_TYPE);

			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for indexing workflow: {}", endTime - startTime, workflow.getWorkflowId());
			Monitors.recordESIndexTime("index_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
			Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
		} catch (Exception e) {
			Monitors.error(className, "indexWorkflow");
			logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
		}
	}

	@Override
	public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
		return CompletableFuture.runAsync(() -> indexWorkflow(workflow), executorService);
	}

	@Override
	public void indexTask(Task task) {
		try {
			long startTime = Instant.now().toEpochMilli();
			String taskId = task.getTaskId();
			TaskSummary summary = new TaskSummary(task);
			byte[] doc = objectMapper.writeValueAsBytes(summary);

			logger.debug("Indexing task document: {} for workflow: {}" + taskId, task.getWorkflowInstanceId());
			UpdateRequest req = new UpdateRequest(indexName, TASK_DOC_TYPE, taskId);
			req.doc(doc);
			req.upsert(doc);
			indexDocument(req, TASK_DOC_TYPE);

			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for  indexing task:{} in workflow: {}", endTime - startTime, task.getTaskId(), task.getWorkflowInstanceId());
			Monitors.recordESIndexTime("index_task", TASK_DOC_TYPE, endTime - startTime);
			Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
		} catch (Exception e) {
			Monitors.error(className, "indexTask");
			logger.error("Failed to index task: {}", task.getTaskId(), e);
		}
	}

	@Override
	public CompletableFuture<Void> asyncIndexTask(Task task) {
		return CompletableFuture.runAsync(() -> indexTask(task), executorService);
	}

	@Override
	public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
		if (taskExecLogs.isEmpty()) {
			return;
		}

		try {
			long startTime = Instant.now().toEpochMilli();
			BulkRequestBuilder bulkRequestBuilder = elasticSearchClient.prepareBulk();
			for (TaskExecLog taskExecLog : taskExecLogs) {
				IndexRequest request = new IndexRequest(logIndexName, LOG_DOC_TYPE);
				request.source(objectMapper.writeValueAsBytes(taskExecLog));
				bulkRequestBuilder.add(request);
			}
			new RetryUtil<BulkResponse>().retryOnException(
				() -> bulkRequestBuilder.execute().actionGet(5, TimeUnit.SECONDS),
				null,
				BulkResponse::hasFailures,
				RETRY_COUNT,
				"Indexing task execution logs",
				"addTaskExecutionLogs"
			);
			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for indexing taskExecutionLogs", endTime - startTime);
			Monitors.recordESIndexTime("index_task_execution_logs", LOG_DOC_TYPE, endTime - startTime);
			Monitors.recordWorkerQueueSize("logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
		} catch (Exception e) {
			List<String> taskIds = taskExecLogs.stream()
				.map(TaskExecLog::getTaskId)
				.collect(Collectors.toList());
			logger.error("Failed to index task execution logs for tasks: {}", taskIds, e);
		}
	}

	@Override
	public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
		return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs), logExecutorService);
	}

	@Override
	public List<TaskExecLog> getTaskExecutionLogs(String taskId) {

		try {

			QueryBuilder qf;
			Expression expression = Expression.fromString("taskId='" + taskId + "'");
			qf = expression.getFilterBuilder();

			BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(qf);
			QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
			BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

			final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(logIndexPrefix + "*").setQuery(fq).setTypes(LOG_DOC_TYPE).addSort(SortBuilders.fieldSort("createdTime").order(SortOrder.ASC).unmappedType("long"));
			SearchResponse response = srb.execute().actionGet();
			SearchHit[] hits = response.getHits().getHits();
			List<TaskExecLog> logs = new ArrayList<>(hits.length);
			for (SearchHit hit : hits) {
				String source = hit.getSourceAsString();
				TaskExecLog tel = objectMapper.readValue(source, TaskExecLog.class);
				logs.add(tel);
			}

			return logs;

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

		return null;
	}

	@Override
	public void addMessage(String queue, Message msg) {
		try {
			long startTime = Instant.now().toEpochMilli();
			Map<String, Object> doc = new HashMap<>();
			doc.put("messageId", msg.getId());
			doc.put("payload", msg.getPayload());
			doc.put("queue", queue);
			doc.put("created", System.currentTimeMillis());

			logger.debug("Indexing message document: {}", msg.getId());
			UpdateRequest request = new UpdateRequest(logIndexName, MSG_DOC_TYPE, msg.getId());
			request.doc(doc, XContentType.JSON);
			request.upsert(doc, XContentType.JSON);
			indexDocument(request, MSG_DOC_TYPE);

			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for  indexing message: {}", endTime - startTime, msg.getId());
			Monitors.recordESIndexTime("add_message", MSG_DOC_TYPE, endTime - startTime);
		}catch (Exception e) {
			Monitors.error(className, "addMessage");
			logger.error("Failed to index message: {}", msg.getId(), e);
		}
	}

	@Override
	public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
		return CompletableFuture.runAsync(() -> addMessage(queue, message), executorService);
	}

	@Override
	public void addEventExecution(EventExecution eventExecution) {
		try {
			long startTime = Instant.now().toEpochMilli();
			byte[] doc = objectMapper.writeValueAsBytes(eventExecution);
			String id = eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution.getMessageId() + "." + eventExecution.getId();

			logger.debug("Indexing event document: {}", id);
			UpdateRequest req = new UpdateRequest(logIndexName, EVENT_DOC_TYPE, id);
			req.doc(doc);
			req.upsert(doc);
			req.retryOnConflict(5);
			indexDocument(req, EVENT_DOC_TYPE);

			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for indexing event execution: {}", endTime - startTime, eventExecution.getId());
			Monitors.recordESIndexTime("add_event_execution", EVENT_DOC_TYPE, endTime - startTime);
		} catch (Exception e) {
			Monitors.error(className, "addEventExecution");
			logger.error("Failed to index event execution: {}", eventExecution.getId(), e);
		}
	}

	@Override
	public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
		return CompletableFuture.runAsync(() -> addEventExecution(eventExecution), logExecutorService);
	}

	private void indexDocument(UpdateRequest request, String docType) {
		new RetryUtil<UpdateResponse>().retryOnException(
			() -> elasticSearchClient.update(request).actionGet(5, TimeUnit.SECONDS),
			null,
			null,
			RETRY_COUNT,
			"Indexing document of type - " + docType,
			"indexDocument"
		);
	}

	@Override
	public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
		try {
			return search(query, start, count, sort, freeText, WORKFLOW_DOC_TYPE);
		} catch (ParserException e) {
			throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
		try {
			return search(query, start, count, sort, freeText, TASK_DOC_TYPE);
		} catch (ParserException e) {
			throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public void removeWorkflow(String workflowId) {
		try {
			long startTime = Instant.now().toEpochMilli();
			DeleteRequest req = new DeleteRequest(indexName, WORKFLOW_DOC_TYPE, workflowId);
			DeleteResponse response = elasticSearchClient.delete(req).actionGet();
			if (!response.isFound()) {
				logger.error("Index removal failed - document not found by id " + workflowId);
			}
			long endTime = Instant.now().toEpochMilli();
			logger.debug("Time taken {} for removing workflow: {}", endTime - startTime, workflowId);
			Monitors.recordESIndexTime("remove_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
			Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
		} catch (Exception e) {
			logger.error("Failed to remove workflow {} from index", workflowId, e);
			Monitors.error(className, "removeWorkflow");
		}
	}

	@Override
	public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
		return CompletableFuture.runAsync(() -> removeWorkflow(workflowId), executorService);
	}

if t.TYPE_CHECKING:  # pragma: no cover
    from .app import Flask

DeferredSetupFunction = t.Callable[["BlueprintSetupState"], t.Callable]
T_after_request = t.TypeVar("T_after_request", bound=ft.AfterRequestCallable)
T_before_first_request = t.TypeVar(
    "T_before_first_request", bound=ft.BeforeFirstRequestCallable
)
T_before_request = t.TypeVar("T_before_request", bound=ft.BeforeRequestCallable)
T_error_handler = t.TypeVar("T_error_handler", bound=ft.ErrorHandlerCallable)
T_teardown = t.TypeVar("T_teardown", bound=ft.TeardownCallable)
T_template_context_processor = t.TypeVar(
    "T_template_context_processor", bound=ft.TemplateContextProcessorCallable
)
T_template_filter = t.TypeVar("T_template_filter", bound=ft.TemplateFilterCallable)
T_template_global = t.TypeVar("T_template_global", bound=ft.TemplateGlobalCallable)
T_template_test = t.TypeVar("T_template_test", bound=ft.TemplateTestCallable)
T_url_defaults = t.TypeVar("T_url_defaults", bound=ft.URLDefaultCallable)
T_url_value_preprocessor = t.TypeVar(
    "T_url_value_preprocessor", bound=ft.URLValuePreprocessorCallable
)


class BlueprintSetupState:
    """Temporary holder object for registering a blueprint with the
    application.  An instance of this class is created by the
    :meth:`~flask.Blueprint.make_setup_state` method and later passed
    to all register callback functions.
    """

    def __init__(
        self,
        blueprint: "Blueprint",
        app: "Flask",
        options: t.Any,
        first_registration: bool,
    ) -> None:
        #: a reference to the current application
        self.app = app

        #: a reference to the blueprint that created this setup state.
        self.blueprint = blueprint

        #: a dictionary with all options that were passed to the
        #: :meth:`~flask.Flask.register_blueprint` method.
        self.options = options

        #: as blueprints can be registered multiple times with the
        #: application and not everything wants to be registered
        #: multiple times on it, this attribute can be used to figure
        #: out if the blueprint was registered in the past already.
        self.first_registration = first_registration

        subdomain = self.options.get("subdomain")
        if subdomain is None:
            subdomain = self.blueprint.subdomain

        #: The subdomain that the blueprint should be active for, ``None``
        #: otherwise.
        self.subdomain = subdomain

        url_prefix = self.options.get("url_prefix")
        if url_prefix is None:
            url_prefix = self.blueprint.url_prefix
        #: The prefix that should be used for all URLs defined on the
        #: blueprint.
        self.url_prefix = url_prefix

        self.name = self.options.get("name", blueprint.name)
        self.name_prefix = self.options.get("name_prefix", "")

        #: A dictionary with URL defaults that is added to each and every
        #: URL that was defined with the blueprint.
        self.url_defaults = dict(self.blueprint.url_values_defaults)
        self.url_defaults.update(self.options.get("url_defaults", ()))

    def add_url_rule(
        self,
        rule: str,
        endpoint: t.Optional[str] = None,
        view_func: t.Optional[t.Callable] = None,
        **options: t.Any,
    ) -> None:
        """A helper method to register a rule (and optionally a view function)
        to the application.  The endpoint is automatically prefixed with the
        blueprint's name.
        """
        if self.url_prefix is not None:
            if rule:
                rule = "/".join((self.url_prefix.rstrip("/"), rule.lstrip("/")))
            else:
                rule = self.url_prefix
        options.setdefault("subdomain", self.subdomain)
        if endpoint is None:
            endpoint = _endpoint_from_view_func(view_func)  # type: ignore
        defaults = self.url_defaults
        if "defaults" in options:
            defaults = dict(defaults, **options.pop("defaults"))

        self.app.add_url_rule(
            rule,
            f"{self.name_prefix}.{self.name}.{endpoint}".lstrip("."),
            view_func,
            defaults=defaults,
            **options,
        )


class Blueprint(Scaffold):
    """Represents a blueprint, a collection of routes and other
    app-related functions that can be registered on a real application
    later.

    A blueprint is an object that allows defining application functions
    without requiring an application object ahead of time. It uses the
    same decorators as :class:`~flask.Flask`, but defers the need for an
    application by recording them for later registration.

    Decorating a function with a blueprint creates a deferred function
    that is called with :class:`~flask.blueprints.BlueprintSetupState`
    when the blueprint is registered on an application.

    See :doc:`/blueprints` for more information.

    :param name: The name of the blueprint. Will be prepended to each
        endpoint name.
    :param import_name: The name of the blueprint package, usually
        ``__name__``. This helps locate the ``root_path`` for the
        blueprint.
    :param static_folder: A folder with static files that should be
        served by the blueprint's static route. The path is relative to
        the blueprint's root path. Blueprint static files are disabled
        by default.
    :param static_url_path: The url to serve static files from.
        Defaults to ``static_folder``. If the blueprint does not have
        a ``url_prefix``, the app's static route will take precedence,
        and the blueprint's static files won't be accessible.
    :param template_folder: A folder with templates that should be added
        to the app's template search path. The path is relative to the
        blueprint's root path. Blueprint templates are disabled by
        default. Blueprint templates have a lower precedence than those
        in the app's templates folder.
    :param url_prefix: A path to prepend to all of the blueprint's URLs,
        to make them distinct from the rest of the app's routes.
    :param subdomain: A subdomain that blueprint routes will match on by
        default.
    :param url_defaults: A dict of default values that blueprint routes
        will receive by default.
    :param root_path: By default, the blueprint will automatically set
        this based on ``import_name``. In certain situations this
        automatic detection can fail, so the path can be specified
        manually instead.

    .. versionchanged:: 1.1.0
        Blueprints have a ``cli`` group to register nested CLI commands.
        The ``cli_group`` parameter controls the name of the group under
        the ``flask`` command.

    .. versionadded:: 0.7
    """

    _got_registered_once = False

    _json_encoder: t.Union[t.Type[json.JSONEncoder], None] = None
    _json_decoder: t.Union[t.Type[json.JSONDecoder], None] = None

    @property
    def json_encoder(
        self,
    ) -> t.Union[t.Type[json.JSONEncoder], None]:
        """Blueprint-local JSON encoder class to use. Set to ``None`` to use the app's.

        .. deprecated:: 2.2
             Will be removed in Flask 2.3. Customize
             :attr:`json_provider_class` instead.

        .. versionadded:: 0.10
        """
        import warnings

        warnings.warn(
            "'bp.json_encoder' is deprecated and will be removed in Flask 2.3."
            " Customize 'app.json_provider_class' or 'app.json' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._json_encoder

    @json_encoder.setter
    def json_encoder(self, value: t.Union[t.Type[json.JSONEncoder], None]) -> None:
        import warnings

        warnings.warn(
            "'bp.json_encoder' is deprecated and will be removed in Flask 2.3."
            " Customize 'app.json_provider_class' or 'app.json' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._json_encoder = value

    @property
    def json_decoder(
        self,
    ) -> t.Union[t.Type[json.JSONDecoder], None]:
        """Blueprint-local JSON decoder class to use. Set to ``None`` to use the app's.

        .. deprecated:: 2.2
             Will be removed in Flask 2.3. Customize
             :attr:`json_provider_class` instead.

        .. versionadded:: 0.10
        """
        import warnings

        warnings.warn(
            "'bp.json_decoder' is deprecated and will be removed in Flask 2.3."
            " Customize 'app.json_provider_class' or 'app.json' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._json_decoder

    @json_decoder.setter
    def json_decoder(self, value: t.Union[t.Type[json.JSONDecoder], None]) -> None:
        import warnings

        warnings.warn(
            "'bp.json_decoder' is deprecated and will be removed in Flask 2.3."
            " Customize 'app.json_provider_class' or 'app.json' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._json_decoder = value

    def __init__(
        self,
        name: str,
        import_name: str,
        static_folder: t.Optional[t.Union[str, os.PathLike]] = None,
        static_url_path: t.Optional[str] = None,
        template_folder: t.Optional[t.Union[str, os.PathLike]] = None,
        url_prefix: t.Optional[str] = None,
        subdomain: t.Optional[str] = None,
        url_defaults: t.Optional[dict] = None,
        root_path: t.Optional[str] = None,
        cli_group: t.Optional[str] = _sentinel,  # type: ignore
    ):
        super().__init__(
            import_name=import_name,
            static_folder=static_folder,
            static_url_path=static_url_path,
            template_folder=template_folder,
            root_path=root_path,
        )

        if "." in name:
            raise ValueError("'name' may not contain a dot '.' character.")

        self.name = name
        self.url_prefix = url_prefix
        self.subdomain = subdomain
        self.deferred_functions: t.List[DeferredSetupFunction] = []

        if url_defaults is None:
            url_defaults = {}

        self.url_values_defaults = url_defaults
        self.cli_group = cli_group
        self._blueprints: t.List[t.Tuple["Blueprint", dict]] = []

    def _check_setup_finished(self, f_name: str) -> None:
        if self._got_registered_once:
            import warnings

            warnings.warn(
                f"The setup method '{f_name}' can no longer be called on"
                f" the blueprint '{self.name}'. It has already been"
                " registered at least once, any changes will not be"
                " applied consistently.\n"
                "Make sure all imports, decorators, functions, etc."
                " needed to set up the blueprint are done before"
                " registering it.\n"
                "This warning will become an exception in Flask 2.3.",
                UserWarning,
                stacklevel=3,
            )

    @setupmethod
    def record(self, func: t.Callable) -> None:
        """Registers a function that is called when the blueprint is
        registered on the application.  This function is called with the
        state as argument as returned by the :meth:`make_setup_state`
        method.
        """
        self.deferred_functions.append(func)

    @setupmethod
    def record_once(self, func: t.Callable) -> None:
        """Works like :meth:`record` but wraps the function in another
        function that will ensure the function is only called once.  If the
        blueprint is registered a second time on the application, the
        function passed is not called.
        """

        def wrapper(state: BlueprintSetupState) -> None:
            if state.first_registration:
                func(state)

        self.record(update_wrapper(wrapper, func))

    def make_setup_state(
        self, app: "Flask", options: dict, first_registration: bool = False
    ) -> BlueprintSetupState:
        """Creates an instance of :meth:`~flask.blueprints.BlueprintSetupState`
        object that is later passed to the register callback functions.
        Subclasses can override this to return a subclass of the setup state.
        """
        return BlueprintSetupState(self, app, options, first_registration)

    @setupmethod
    def register_blueprint(self, blueprint: "Blueprint", **options: t.Any) -> None:
        """Register a :class:`~flask.Blueprint` on this blueprint. Keyword
        arguments passed to this method will override the defaults set
        on the blueprint.

        .. versionchanged:: 2.0.1
            The ``name`` option can be used to change the (pre-dotted)
            name the blueprint is registered with. This allows the same
            blueprint to be registered multiple times with unique names
            for ``url_for``.

        .. versionadded:: 2.0
        """
        if blueprint is self:
            raise ValueError("Cannot register a blueprint on itself")
        self._blueprints.append((blueprint, options))

    def register(self, app: "Flask", options: dict) -> None:
        """Called by :meth:`Flask.register_blueprint` to register all
        views and callbacks registered on the blueprint with the
        application. Creates a :class:`.BlueprintSetupState` and calls
        each :meth:`record` callback with it.

        :param app: The application this blueprint is being registered
            with.
        :param options: Keyword arguments forwarded from
            :meth:`~Flask.register_blueprint`.

        .. versionchanged:: 2.0.1
            Nested blueprints are registered with their dotted name.
            This allows different blueprints with the same name to be
            nested at different locations.

        .. versionchanged:: 2.0.1
            The ``name`` option can be used to change the (pre-dotted)
            name the blueprint is registered with. This allows the same
            blueprint to be registered multiple times with unique names
            for ``url_for``.

        .. versionchanged:: 2.0.1
            Registering the same blueprint with the same name multiple
            times is deprecated and will become an error in Flask 2.1.
        """
        name_prefix = options.get("name_prefix", "")
        self_name = options.get("name", self.name)
        name = f"{name_prefix}.{self_name}".lstrip(".")

        if name in app.blueprints:
            bp_desc = "this" if app.blueprints[name] is self else "a different"
            existing_at = f" '{name}'" if self_name != name else ""

            raise ValueError(
                f"The name '{self_name}' is already registered for"
                f" {bp_desc} blueprint{existing_at}. Use 'name=' to"
                f" provide a unique name."
            )

        first_bp_registration = not any(bp is self for bp in app.blueprints.values())
        first_name_registration = name not in app.blueprints

        app.blueprints[name] = self
        self._got_registered_once = True
        state = self.make_setup_state(app, options, first_bp_registration)

        if self.has_static_folder:
            state.add_url_rule(
                f"{self.static_url_path}/<path:filename>",
                view_func=self.send_static_file,
                endpoint="static",
            )

        # Merge blueprint data into parent.
        if first_bp_registration or first_name_registration:

            def extend(bp_dict, parent_dict):
                for key, values in bp_dict.items():
                    key = name if key is None else f"{name}.{key}"
                    parent_dict[key].extend(values)

            for key, value in self.error_handler_spec.items():
                key = name if key is None else f"{name}.{key}"
                value = defaultdict(
                    dict,
                    {
                        code: {
                            exc_class: func for exc_class, func in code_values.items()
                        }
                        for code, code_values in value.items()
                    },
                )
                app.error_handler_spec[key] = value

            for endpoint, func in self.view_functions.items():
                app.view_functions[endpoint] = func

            extend(self.before_request_funcs, app.before_request_funcs)
            extend(self.after_request_funcs, app.after_request_funcs)
            extend(
                self.teardown_request_funcs,
                app.teardown_request_funcs,
            )
            extend(self.url_default_functions, app.url_default_functions)
            extend(self.url_value_preprocessors, app.url_value_preprocessors)
            extend(self.template_context_processors, app.template_context_processors)

        for deferred in self.deferred_functions:
            deferred(state)

        cli_resolved_group = options.get("cli_group", self.cli_group)

        if self.cli.commands:
            if cli_resolved_group is None:
                app.cli.commands.update(self.cli.commands)
            elif cli_resolved_group is _sentinel:
                self.cli.name = name
                app.cli.add_command(self.cli)
            else:
                self.cli.name = cli_resolved_group
                app.cli.add_command(self.cli)

        for blueprint, bp_options in self._blueprints:
            bp_options = bp_options.copy()
            bp_url_prefix = bp_options.get("url_prefix")

            if bp_url_prefix is None:
                bp_url_prefix = blueprint.url_prefix

            if state.url_prefix is not None and bp_url_prefix is not None:
                bp_options["url_prefix"] = (
                    state.url_prefix.rstrip("/") + "/" + bp_url_prefix.lstrip("/")
                )
            elif bp_url_prefix is not None:
                bp_options["url_prefix"] = bp_url_prefix
            elif state.url_prefix is not None:
                bp_options["url_prefix"] = state.url_prefix

            bp_options["name_prefix"] = name
            blueprint.register(app, bp_options)

    @setupmethod
    def add_url_rule(
        self,
        rule: str,
        endpoint: t.Optional[str] = None,
        view_func: t.Optional[ft.RouteCallable] = None,
        provide_automatic_options: t.Optional[bool] = None,
        **options: t.Any,
    ) -> None:
        """Like :meth:`Flask.add_url_rule` but for a blueprint.  The endpoint for
        the :func:`url_for` function is prefixed with the name of the blueprint.
        """
        if endpoint and "." in endpoint:
            raise ValueError("'endpoint' may not contain a dot '.' character.")

        if view_func and hasattr(view_func, "__name__") and "." in view_func.__name__:
            raise ValueError("'view_func' name may not contain a dot '.' character.")

        self.record(
            lambda s: s.add_url_rule(
                rule,
                endpoint,
                view_func,
                provide_automatic_options=provide_automatic_options,
                **options,
            )
        )

    @setupmethod
    def app_template_filter(
        self, name: t.Optional[str] = None
    ) -> t.Callable[[T_template_filter], T_template_filter]:
        """Register a custom template filter, available application wide.  Like
        :meth:`Flask.template_filter` but for a blueprint.

        :param name: the optional name of the filter, otherwise the
                     function name will be used.
        """

        def decorator(f: T_template_filter) -> T_template_filter:
            self.add_app_template_filter(f, name=name)
            return f

        return decorator

    @setupmethod
    def add_app_template_filter(
        self, f: ft.TemplateFilterCallable, name: t.Optional[str] = None
    ) -> None:
        """Register a custom template filter, available application wide.  Like
        :meth:`Flask.add_template_filter` but for a blueprint.  Works exactly
        like the :meth:`app_template_filter` decorator.

        :param name: the optional name of the filter, otherwise the
                     function name will be used.
        """

        def register_template(state: BlueprintSetupState) -> None:
            state.app.jinja_env.filters[name or f.__name__] = f

        self.record_once(register_template)

    @setupmethod
    def app_template_test(
        self, name: t.Optional[str] = None
    ) -> t.Callable[[T_template_test], T_template_test]:
        """Register a custom template test, available application wide.  Like
        :meth:`Flask.template_test` but for a blueprint.

        .. versionadded:: 0.10

        :param name: the optional name of the test, otherwise the
                     function name will be used.
        """

        def decorator(f: T_template_test) -> T_template_test:
            self.add_app_template_test(f, name=name)
            return f

        return decorator

    @setupmethod
    def add_app_template_test(
        self, f: ft.TemplateTestCallable, name: t.Optional[str] = None
    ) -> None:
        """Register a custom template test, available application wide.  Like
        :meth:`Flask.add_template_test` but for a blueprint.  Works exactly
        like the :meth:`app_template_test` decorator.

        .. versionadded:: 0.10

        :param name: the optional name of the test, otherwise the
                     function name will be used.
        """

        def register_template(state: BlueprintSetupState) -> None:
            state.app.jinja_env.tests[name or f.__name__] = f

        self.record_once(register_template)

    @setupmethod
    def app_template_global(
        self, name: t.Optional[str] = None
    ) -> t.Callable[[T_template_global], T_template_global]:
        """Register a custom template global, available application wide.  Like
        :meth:`Flask.template_global` but for a blueprint.

        .. versionadded:: 0.10

        :param name: the optional name of the global, otherwise the
                     function name will be used.
        """

        def decorator(f: T_template_global) -> T_template_global:
            self.add_app_template_global(f, name=name)
            return f

        return decorator

    @setupmethod
    def add_app_template_global(
        self, f: ft.TemplateGlobalCallable, name: t.Optional[str] = None
    ) -> None:
        """Register a custom template global, available application wide.  Like
        :meth:`Flask.add_template_global` but for a blueprint.  Works exactly
        like the :meth:`app_template_global` decorator.

        .. versionadded:: 0.10

        :param name: the optional name of the global, otherwise the
                     function name will be used.
        """

        def register_template(state: BlueprintSetupState) -> None:
            state.app.jinja_env.globals[name or f.__name__] = f

        self.record_once(register_template)

    @setupmethod
    def before_app_request(self, f: T_before_request) -> T_before_request:
        """Like :meth:`Flask.before_request`.  Such a function is executed
        before each request, even if outside of a blueprint.
        """
        self.record_once(
            lambda s: s.app.before_request_funcs.setdefault(None, []).append(f)
        )
        return f

    @setupmethod
    def before_app_first_request(
        self, f: T_before_first_request
    ) -> T_before_first_request:
        """Like :meth:`Flask.before_first_request`.  Such a function is
        executed before the first request to the application.

        .. deprecated:: 2.2
            Will be removed in Flask 2.3. Run setup code when creating
            the application instead.
        """
        import warnings

        warnings.warn(
            "'before_app_first_request' is deprecated and will be"
            " removed in Flask 2.3. Use 'record_once' instead to run"
            " setup code when registering the blueprint.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.record_once(lambda s: s.app.before_first_request_funcs.append(f))
        return f

    @setupmethod
    def after_app_request(self, f: T_after_request) -> T_after_request:
        """Like :meth:`Flask.after_request` but for a blueprint.  Such a function
        is executed after each request, even if outside of the blueprint.
        """
        self.record_once(
            lambda s: s.app.after_request_funcs.setdefault(None, []).append(f)
        )
        return f

    @setupmethod
    def teardown_app_request(self, f: T_teardown) -> T_teardown:
        """Like :meth:`Flask.teardown_request` but for a blueprint.  Such a
        function is executed when tearing down each request, even if outside of
        the blueprint.
        """
        self.record_once(
            lambda s: s.app.teardown_request_funcs.setdefault(None, []).append(f)
        )
        return f

    @setupmethod
    def app_context_processor(
        self, f: T_template_context_processor
    ) -> T_template_context_processor:
        """Like :meth:`Flask.context_processor` but for a blueprint.  Such a
        function is executed each request, even if outside of the blueprint.
        """
        self.record_once(
            lambda s: s.app.template_context_processors.setdefault(None, []).append(f)
        )
        return f

    @setupmethod
    def app_errorhandler(
        self, code: t.Union[t.Type[Exception], int]
    ) -> t.Callable[[T_error_handler], T_error_handler]:
        """Like :meth:`Flask.errorhandler` but for a blueprint.  This
        handler is used for all requests, even if outside of the blueprint.
        """

        def decorator(f: T_error_handler) -> T_error_handler:
            self.record_once(lambda s: s.app.errorhandler(code)(f))
            return f

        return decorator

    @setupmethod
    def app_url_value_preprocessor(
        self, f: T_url_value_preprocessor
    ) -> T_url_value_preprocessor:
        """Same as :meth:`url_value_preprocessor` but application wide."""
        self.record_once(
            lambda s: s.app.url_value_preprocessors.setdefault(None, []).append(f)
        )
        return f

    @setupmethod
    def app_url_defaults(self, f: T_url_defaults) -> T_url_defaults:
        """Same as :meth:`url_defaults` but application wide."""
        self.record_once(
            lambda s: s.app.url_default_functions.setdefault(None, []).append(f)
        )
        return f

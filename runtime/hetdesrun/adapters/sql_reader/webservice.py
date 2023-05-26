"""Web service endpoints for frontend for the sql reader adapter

Note that the sql reader adapter is not a generic rest adapter, so these webendpoints
have the sole purpose to tell the frontend which data sources and sinks are available and
can be wired.

Actual data ingestion/egestion happens in the corresponding Runtime-Python-Plugin of this adapter.
"""


from fastapi import HTTPException, Query

from hetdesrun.adapters.sql_reader import VERSION
from hetdesrun.adapters.sql_reader.models import (
    InfoResponse,
    MultipleSinksResponse,
    MultipleSourcesResponse,
    SQLReaderStructureSource,
    StructureResponse,
    StructureThingNode,
)
from hetdesrun.adapters.sql_reader.structure import (
    get_source_by_id,
    get_sources,
    get_structure,
    get_thing_node_by_id,
)
from hetdesrun.adapters.sql_reader.utils import from_url_representation
from hetdesrun.webservice.auth_dependency import get_auth_deps
from hetdesrun.webservice.router import HandleTrailingSlashAPIRouter

# Note: As CORS middleware the router employs the main FastAPI app's one
sql_reader_adapter_router = HandleTrailingSlashAPIRouter(
    prefix="/adapters/sqlreader", tags=["sql reader adapter"]
)


@sql_reader_adapter_router.get(
    "/info",
    response_model=InfoResponse,
    # no auth for info endpoint
)
async def get_info_endpoint() -> InfoResponse:
    return InfoResponse(
        id="sql-table-reader-adapter", name="SQL Reader Adapter", version=VERSION
    )


@sql_reader_adapter_router.get(
    "/structure",
    response_model=StructureResponse,
    dependencies=get_auth_deps(),
)
async def get_structure_endpoint(parentId: str | None = None) -> StructureResponse:
    return get_structure(parent_id=parentId)


@sql_reader_adapter_router.get(
    "/sources",
    response_model=MultipleSourcesResponse,
    dependencies=get_auth_deps(),
)
async def get_sources_endpoint(
    filter_str: str | None = Query(None, alias="filter")
) -> MultipleSourcesResponse:
    found_sources = get_sources(filter_str=filter_str)
    return MultipleSourcesResponse(
        resultCount=len(found_sources),
        sources=found_sources,
    )


@sql_reader_adapter_router.get(
    "/sinks",
    response_model=MultipleSinksResponse,
    dependencies=get_auth_deps(),
)
async def get_sinks_endpoint(
    filter_str: str | None = Query(None, alias="filter")  # noqa: ARG001
) -> MultipleSinksResponse:
    found_sinks = []
    return MultipleSinksResponse(
        resultCount=len(found_sinks),
        sinks=found_sinks,
    )


@sql_reader_adapter_router.get(
    "/sources/{sourceId}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_sources_metadata(
    sourceId: str,  # noqa: ARG001
) -> list:
    """Get metadata attached to sources

    This adapter does not implement metadata. Therefore this will always result
    in an empty list!
    """
    return []


@sql_reader_adapter_router.get(
    "/sources/{source_id:path}",
    response_model=SQLReaderStructureSource,
    dependencies=get_auth_deps(),
)
async def get_single_source(source_id: str) -> SQLReaderStructureSource:
    possible_source = get_source_by_id(source_id)

    if possible_source is None:
        raise HTTPException(
            status_code=404,
            detail="Could not find loadable sql data "
            + from_url_representation(source_id),
        )

    return possible_source


@sql_reader_adapter_router.get(
    "/sinks/{sinkId}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_sinks_metadata(sinkId: str) -> list:  # noqa: ARG001
    """Get metadata attached to sinks

    This adapter does not implement metadata. Therefore this will always result
    in an empty list!
    """
    return []


@sql_reader_adapter_router.get(
    "/thingNodes/{thingNodeId}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_thing_nodes_metadata(
    thingNodeId: str,  # noqa: ARG001
) -> list:
    """Get metadata attached to thing Nodes.

    This adapter does not implement metadata. Therefore this will always result
    in an empty list!
    """
    return []


@sql_reader_adapter_router.get(
    "/thingNodes/{id}",
    response_model=StructureThingNode,
    dependencies=get_auth_deps(),
)
async def get_single_thingNode(
    id: str,  # noqa: A002
) -> StructureThingNode:
    possible_thing_node = get_thing_node_by_id(id)

    if possible_thing_node is None:
        raise HTTPException(
            status_code=404,
            detail="Could not find sql thing node at id path "
            + from_url_representation(id),
        )

    return possible_thing_node

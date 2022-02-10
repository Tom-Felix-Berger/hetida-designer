from unittest import mock
from copy import deepcopy
import pytest

from starlette.testclient import TestClient
from posixpath import join as posix_urljoin

import json

from hetdesrun.utils import get_uuid_from_seed

from hetdesrun.webservice.application import app

from hetdesrun.persistence import get_db_engine, sessionmaker

from hetdesrun.persistence.dbmodels import Base
from hetdesrun.persistence.dbservice.revision import (
    read_single_transformation_revision,
    store_single_transformation_revision,
)
from hetdesrun.persistence.dbservice.nesting import update_or_create_nesting

from hetdesrun.persistence.models.transformation import TransformationRevision

from hetdesrun.backend.service.transformation_router import generate_code
from hetdesrun.backend.models.wiring import WiringFrontendDto

from hetdesrun.exportimport.importing import load_json

client = TestClient(app)


@pytest.fixture(scope="function")
def clean_test_db_engine(use_in_memory_db):
    if use_in_memory_db:
        in_memory_database_url = "sqlite+pysqlite:///:memory:"
        engine = get_db_engine(override_db_url=in_memory_database_url)
    else:
        engine = get_db_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


tr_json_component_1 = {
    "id": str(get_uuid_from_seed("component 1")),
    "revision_group_id": str(get_uuid_from_seed("group of component 1")),
    "name": "component 0",
    "description": "description of component 0",
    "category": "category",
    "documentation": "documentation",
    "type": "COMPONENT",
    "state": "DRAFT",
    "version_tag": "1.0.0",
    "io_interface": {
        "inputs": [
            {
                "id": str(get_uuid_from_seed("operator input")),
                "name": "operator_input",
                "data_type": "INT",
            }
        ],
        "outputs": [
            {
                "id": str(get_uuid_from_seed("operator output")),
                "name": "operator_output",
                "data_type": "INT",
            }
        ],
    },
    "content": "code",
    "test_wiring": {"input_wirings": [], "output_wirings": [],},
}
tr_json_component_2 = {
    "id": str(get_uuid_from_seed("component 2")),
    "revision_group_id": str(get_uuid_from_seed("group of component 2")),
    "name": "component 2",
    "description": "description of component 2",
    "category": "category",
    "documentation": "documentation",
    "type": "COMPONENT",
    "state": "RELEASED",
    "released_timestamp": "2019-12-01T12:00:00+00:00",
    "version_tag": "1.0.0",
    "io_interface": {"inputs": [], "outputs": [],},
    "content": "code",
    "test_wiring": {"input_wirings": [], "output_wirings": [],},
}
tr_json_component_2_update = {
    "id": str(get_uuid_from_seed("component 2")),
    "revision_group_id": str(get_uuid_from_seed("group of component 2")),
    "name": "new name",
    "description": "description of component 2",
    "category": "Test",
    "documentation": "documentation",
    "type": "COMPONENT",
    "state": "RELEASED",
    "released_timestamp": "2019-12-01T12:00:00+00:00",
    "version_tag": "1.0.0",
    "io_interface": {"inputs": [], "outputs": [],},
    "content": "code",
    "test_wiring": {"input_wirings": [], "output_wirings": [],},
}
tr_json_component_2_deprecate = {
    "id": str(get_uuid_from_seed("component 2")),
    "revision_group_id": str(get_uuid_from_seed("group of component 2")),
    "name": "new name",
    "description": "description of component 2",
    "category": "Test",
    "documentation": "documentation",
    "type": "COMPONENT",
    "state": "DISABLED",
    "released_timestamp": "2019-12-01T12:00:00+00:00",
    "disabled_timestamp": "2023-08-03T12:00:00+00:00",
    "version_tag": "1.0.0",
    "io_interface": {"inputs": [], "outputs": [],},
    "content": "code",
    "test_wiring": {"input_wirings": [], "output_wirings": [],},
}
tr_json_workflow_1 = {
    "id": str(get_uuid_from_seed("workflow 1")),
    "revision_group_id": str(get_uuid_from_seed("group of workflow 1")),
    "name": "workflow 1",
    "description": "description of workflow 1",
    "category": "category",
    "documentation": "documentation",
    "type": "WORKFLOW",
    "state": "DRAFT",
    "version_tag": "1.0.0",
    "io_interface": {"inputs": [], "outputs": [],},
    "content": {
        "constants": [],
        "inputs": [],
        "outputs": [],
        "operators": [],
        "links": [],
    },
    "test_wiring": {"input_wirings": [], "output_wirings": [],},
}

tr_json_workflow_2 = {
    "id": str(get_uuid_from_seed("workflow 2")),
    "revision_group_id": str(get_uuid_from_seed("group of workflow 2")),
    "name": "workflow 2",
    "description": "description of workflow 2",
    "category": "category",
    "documentation": "documentation",
    "type": "WORKFLOW",
    "state": "DRAFT",
    "version_tag": "1.0.0",
    "io_interface": {
        "inputs": [
            {
                "id": str(get_uuid_from_seed("input")),
                "name": "wf_input",
                "data_type": "INT",
            }
        ],
        "outputs": [
            {
                "id": str(get_uuid_from_seed("output")),
                "name": "wf_output",
                "data_type": "INT",
            }
        ],
    },
    "content": {
        "constants": [],
        "inputs": [
            {
                "id": str(get_uuid_from_seed("input")),
                "name": "wf_input",
                "data_type": "INT",
                "position": {"x": 0, "y": 0},
            }
        ],
        "outputs": [
            {
                "id": str(get_uuid_from_seed("output")),
                "name": "wf_output",
                "data_type": "INT",
                "position": {"x": 0, "y": 0},
            }
        ],
        "operators": [
            {
                "id": str(get_uuid_from_seed("operator")),
                "revision_group_id": str(get_uuid_from_seed("group of component 1")),
                "name": "operator",
                "description": "",
                "category": "category",
                "type": "COMPONENT",
                "state": "RELEASED",
                "version_tag": "1.0.0",
                "transformation_id": str(get_uuid_from_seed("component 1")),
                "inputs": [
                    {
                        "id": str(get_uuid_from_seed("operator input")),
                        "name": "operator_input",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                ],
                "outputs": [
                    {
                        "id": str(get_uuid_from_seed("operator output")),
                        "name": "operator_output",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                ],
                "position": {"x": 0, "y": 0},
            }
        ],
        "links": [
            {
                "id": str(get_uuid_from_seed("link 1")),
                "start": {
                    "connector": {
                        "id": str(get_uuid_from_seed("input")),
                        "name": "wf_input",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                },
                "end": {
                    "operator": str(get_uuid_from_seed("operator")),
                    "connector": {
                        "id": str(get_uuid_from_seed("operator input")),
                        "name": "operator_input",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                },
                "path": [],
            },
            {
                "id": str(get_uuid_from_seed("link 2")),
                "start": {
                    "operator": str(get_uuid_from_seed("operator")),
                    "connector": {
                        "id": str(get_uuid_from_seed("operator output")),
                        "name": "operator_output",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                },
                "end": {
                    "connector": {
                        "id": str(get_uuid_from_seed("output")),
                        "name": "wf_output",
                        "data_type": "INT",
                        "position": {"x": 0, "y": 0},
                    },
                },
                "path": [],
            },
        ],
    },
    "test_wiring": {
        "input_wirings": [
            {
                "workflow_input_name": "wf_input",
                "adapter_id": "direct_provisioning",
                "filters": {"value": 100},
            },
        ],
        "output_wirings": [
            {"workflow_output_name": "wf_output", "adapter_id": "direct_provisioning",},
        ],
    },
}
tr_json_workflow_2_update = deepcopy(tr_json_workflow_2)
tr_json_workflow_2_update["name"] = "new name"


@pytest.mark.asyncio
async def test_get_all_transformation_revisions_with_valid_db_entries(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_1)
        )
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_2)
        )
        store_single_transformation_revision(
            TransformationRevision(**tr_json_workflow_1)
        )
        store_single_transformation_revision(
            TransformationRevision(**tr_json_workflow_2)
        )
        async with async_test_client as ac:
            response = await ac.get("/api/transformations/")

        assert response.status_code == 200
        assert response.json()[0] == tr_json_component_1
        assert response.json()[1] == tr_json_component_2
        assert response.json()[2] == tr_json_workflow_1
        assert response.json()[3] == tr_json_workflow_2


@pytest.mark.asyncio
async def test_get_transformation_revision_by_id_with_component(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_1)
        )

        async with async_test_client as ac:
            response = await ac.get(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("component 1"))
                )
            )
        assert response.status_code == 200
        assert response.json() == tr_json_component_1


@pytest.mark.asyncio
async def test_get_transformation_revision_by_id_with_inexistent_component(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        async with async_test_client as ac:
            response = await ac.get(
                posix_urljoin(
                    "/api/transformations/",
                    str(get_uuid_from_seed("inexistent transformation revision")),
                )
            )
        assert response.status_code == 404
        assert "Found no" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_transformation_revision_by_id_with_workflow(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_workflow_1)
        )

        async with async_test_client as ac:
            response = await ac.get(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("workflow 1"))
                )
            )
        assert response.status_code == 200
        assert response.json() == tr_json_workflow_1


@pytest.mark.asyncio
async def test_get_transformation_revision_by_id_with_inexistent_workflow(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        async with async_test_client as ac:
            response = await ac.get(
                posix_urljoin(
                    "/api/transformations/",
                    str(get_uuid_from_seed("inexistent workflow")),
                )
            )
        assert response.status_code == 404
        assert "Found no" in response.json()["detail"]


@pytest.mark.asyncio
async def test_create_transformation_revision_with_workflow(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):

        async with async_test_client as ac:
            response = await ac.post("/api/transformations/", json=tr_json_workflow_2)

        assert response.status_code == 201
        assert response.json()["name"] == tr_json_workflow_2["name"]


@pytest.mark.asyncio
async def test_update_transformation_revision_with_workflow(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_workflow_2)
        )

        async with async_test_client as ac:
            response = await ac.put(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("workflow 2"))
                ),
                json=tr_json_workflow_2_update,
            )

        workflow_tr_in_db = read_single_transformation_revision(
            get_uuid_from_seed("workflow 2")
        )

        assert response.status_code == 201
        assert response.json()["name"] == "new name"
        assert len(workflow_tr_in_db.content.links) == 2


@pytest.mark.asyncio
async def test_update_transformation_revision_with_non_existing_workflow(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        async with async_test_client as ac:
            response = await ac.put(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("workflow 2"))
                ),
                json=tr_json_workflow_2_update,
            )

        workflow_tr_in_db = read_single_transformation_revision(
            get_uuid_from_seed("workflow 2")
        )

        assert response.status_code == 201
        assert response.json()["name"] == "new name"
        assert len(workflow_tr_in_db.content.links) == 2


@pytest.mark.asyncio
async def test_update_transformation_revision_with_released_component(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_2)
        )

        async with async_test_client as ac:
            response = await ac.put(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("component 2"))
                ),
                json=tr_json_component_2_update,
            )

        assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_transformation_revision_with_released_component_and_allow_overwrite_flag(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_2)
        )

        async with async_test_client as ac:
            response = await ac.put(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("component 2"))
                )
                + "?allow_overwrite_released=true",
                json=tr_json_component_2_update,
            )

        assert response.status_code == 201
        assert response.json()["name"] == "new name"
        assert response.json()["category"] == "Test"


@pytest.mark.asyncio
async def test_deprecate_transformation_revision_with_component(
    async_test_client, clean_test_db_engine
):
    with mock.patch(
        "hetdesrun.persistence.dbservice.revision.Session",
        sessionmaker(clean_test_db_engine),
    ):
        store_single_transformation_revision(
            TransformationRevision(**tr_json_component_2)
        )

        async with async_test_client as ac:
            response = await ac.put(
                posix_urljoin(
                    "/api/transformations/", str(get_uuid_from_seed("component 2"))
                ),
                json=tr_json_component_2_deprecate,
            )

        assert response.status_code == 201
        assert response.json()["name"] != "new name"
        assert response.json()["category"] != "Test"


@pytest.mark.asyncio
async def test_execute_for_transformation_revision(
    async_test_client, clean_test_db_engine
):
    patched_session = sessionmaker(clean_test_db_engine)
    with mock.patch(
        "hetdesrun.persistence.dbservice.nesting.Session", patched_session,
    ):
        with mock.patch(
            "hetdesrun.persistence.dbservice.revision.Session", patched_session,
        ):
            tr_component_1 = TransformationRevision(**tr_json_component_1)
            tr_component_1.content = generate_code(tr_component_1.to_code_body())
            store_single_transformation_revision(tr_component_1)
            tr_workflow_2 = TransformationRevision(**tr_json_workflow_2_update)

            store_single_transformation_revision(tr_workflow_2)

            update_or_create_nesting(tr_workflow_2)

            async with async_test_client as ac:
                response = await ac.post(
                    posix_urljoin(
                        "/api/transformations/",
                        str(get_uuid_from_seed("workflow 2")),
                        "execute",
                    ),
                    json=json.loads(tr_workflow_2.test_wiring.json()),
                )

            assert response.status_code == 200
            assert "output_types_by_output_name" in response.json()


@pytest.mark.asyncio
async def test_import_transformation(async_test_client, clean_test_db_engine):
    patched_session = sessionmaker(clean_test_db_engine)
    with mock.patch(
        "hetdesrun.persistence.dbservice.nesting.Session", patched_session,
    ):
        with mock.patch(
            "hetdesrun.persistence.dbservice.revision.Session", patched_session,
        ):

            example_workflow_tr_json = load_json(
                "./transformations/workflows/examples/data-from-last-positive-step_100_2cbb87e7-ea99-4404-abe1-be550f22763f.json"
            )

            async with async_test_client as ac:
                response = await ac.put(
                    posix_urljoin(
                        "/api/transformations/", example_workflow_tr_json["id"]
                    ),
                    json=example_workflow_tr_json,
                )

            assert response.status_code == 201
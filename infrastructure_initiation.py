import config
import tasks
from logger_infrastructure import *



if __name__ == '__main__':
    client = tasks.create_client(cred_json=config.cred_json, project_id=config.project_id)
    logging.info(f"Client object has been created in {config.project_id}")

    # creating the schema
    tasks.create_schema(
        client=client, project_id=config.project_id, dataset_id=config.dataset_id
    )
    logging.info(f"Schema {config.dataset_id }has been created in {config.project_id}")

    # dropping tables if they exist
    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="badges_fact",
    )
    logging.info("badges_fact has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="comments_fact",
    )
    logging.info("comments_fact has been dropped")
    
    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="",
    )
    logging.info("post_answers_fact has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_answers_fact",
    )
    logging.info("post_answers_fact has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_history_fact",
    )
    logging.info("post_history_fact has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_links_fact",
    )
    logging.info("post_links_fact has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="users_dim",
    )
    logging.info("users_dim has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="tags_dim",
    )
    logging.info("tags_dim has been dropped")

    tasks.drop_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="posts_question_dim",
    )
    logging.info("posts_question_dim has been dropped")

    # creating tables
    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="badges_fact",
    )
    logging.info("badges_fact has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="comments_fact",
    )
    logging.info("comments_fact has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="comments_fact",
    )
    logging.info("DimPlatform_SCD1 has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_answers_fact",
    )
    logging.info("post_answers_fact has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_link_fact",
    )
    logging.info("post_links_fact has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="users_dim",
    )
    logging.info("users_dim has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="tags_dim",
    )
    logging.info("tags_dim has been created")

    tasks.create_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_name="post_question_dim",
    )
    logging.info("posts_question_dim has been created")

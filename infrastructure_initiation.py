import config
import tasks


if __name__ == '__main__':
    client = tasks.create_client(cred_json=config.cred_json, project_id=config.project_id)

    # dropping tables if they exist
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='users_dim')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='tags_dim')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='posts_question_dim')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='badges_fact')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='comments_fact')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='post_answers_fact')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='post_history_fact')
    tasks.drop_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                     table_name='post_links_fact')

    # creating tables
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='users_dim')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='tags_dim')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='posts_question_dim')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='badges_fact')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='comments_fact')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='post_answers_fact')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='post_history_fact')
    tasks.create_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       table_name='post_links_fact')




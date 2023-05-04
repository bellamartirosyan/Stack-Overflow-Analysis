import config
import tasks


if __name__ == '__main__':
    client = tasks.create_client(cred_json=config.cred_json, project_id=config.project_id)

    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='users_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='tags_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='posts_question_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='badges_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='comments_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_answers_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_history_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_links_fact', src_table_name=config.table_name)


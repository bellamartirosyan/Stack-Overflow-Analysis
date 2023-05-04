MERGE `velvety-ring-349218.Stack_Overflow.badges` AS target
USING
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.badges`)
AS source
ON target.badge_id = source.badge_id
WHEN NOT MATCHED THEN
  INSERT (dim_badge_sk, staging_raw_badge_id, name, date, user_id, class, tag_based)
  VALUES (source.sk, source.badge_id, source.name, source.date, source.user_id, source.class, source.tag_based)
WHEN MATCHED THEN
  UPDATE SET
    name = source.name,
    date = source.date,
    user_id = source.user_id,
    class = source.class,
    tag_based = source.tag_based;

--merge comments
MERGE `velvety-ring-349218.Stack_Overflow.comments` AS target
USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.comments`) 
AS source
ON target.comment_id = source.comment_id
WHEN NOT MATCHED THEN
  INSERT (comment_id, text, creation_date, post_id, user_id, user_display_name, score)
  VALUES (source.comment_id, source.text, source.creation_date, source.post_id, source.user_id, source.user_display_name, source.score)
WHEN MATCHED THEN
  UPDATE SET
    text = source.text,
    creation_date = source.creation_date,
    post_id = source.post_id,
    user_id = source.user_id,
    user_display_name = source.user_display_name,
    score = source.score;
--merge post_history
MERGE `velvety-ring-349218.Stack_Overflow.post_history` AS target
USING
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.post_history`) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
  INSERT (id, creation_date, post_id, revision_guid, user_id, text, comment)
  VALUES (source.id, source.creation_date, source.post_id, source.revision_guid, source.user_id, source.text, source.comment)
WHEN MATCHED THEN
  UPDATE SET
    creation_date = source.creation_date,
    post_id = source.post_id,
    revision_guid = source.revision_guid,
    user_id = source.user_id,
    text = source.text,
    comment = source.comment;
--merge post_links
MERGE `velvety-ring-349218.Stack_Overflow.post_links` AS target
USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.post_links`)
AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
  INSERT (id, creation_date, link_type_id, post_id, related_post_id)
  VALUES (source.id, source.creation_date, source.link_type_id, source.post_id, source.related_post_id)
WHEN MATCHED THEN
  UPDATE SET
    creation_date = source.creation_date,
    link_type_id = source.link_type_id,
    post_id = source.post_id,
    related_post_id = source.related_post_id;
--merge post_answers
MERGE `velvety-ring-349218.Stack_Overflow.post_answers` AS target
USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.post_answers`) 
AS source
ON target.post_answer_id = source.post_answer_id
WHEN NOT MATCHED THEN
  INSERT (post_answer_id, title, body, answer_count, comment_count, community_owned_date, creation_date, favorite_count, last_activity_date, last_edit_date, post_id, score, tags, view_count)
  VALUES (source.post_answer_id, source.title, source.body, source.answer_count, source.comment_count, source.community_owned_date, source.creation_date, source.favorite_count, source.last_activity_date, source.last_edit_date, source.post_id, source.score, source.tags, source.view_count)
WHEN MATCHED THEN
  UPDATE SET
    target.title = source.title,
    target.body = source.body,
    target.answer_count = source.answer_count,
    target.comment_count = source.comment_count,
    target.community_owned_date = source.community_owned_date,
    target.creation_date = source.creation_date,
    target.favorite_count = source.favorite_count,
    target.last_activity_date = source.last_activity_date,
    target.last_edit_date = source.last_edit_date,
    target.post_id = source.post_id,
    target.score = source.score,
    target.tags = source.tags,
    target.view_count = source.view_count

    -- Merge tags
MERGE `velvety-ring-349218.Stack_Overflow.tags` AS target
USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.tags`)
AS source
ON target.tag_id = source.tag_id
WHEN MATCHED THEN
  UPDATE SET
    tag_name = source.tag_name,
    count = source.count,
    excerpt_post_id = source.excerpt_post_id,
    wiki_post_id = source.wiki_post_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    tag_id,
    tag_name,
    count,
    excerpt_post_id,
    wiki_post_id
  ) VALUES (
    source.tag_id,
    source.tag_name,
    source.count,
    source.excerpt_post_id,
    source.wiki_post_id
  );

-- Merge users
MERGE `velvety-ring-349218.Stack_Overflow.users` AS target
USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.users`)
 AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
  UPDATE SET
    display_name = source.display_name,
    about_me = source.about_me,
    age = source.age,
    creation_date = source.creation_date,
    last_access_date = source.last_access_date,
    location = source.location,
    reputation = source.reputation,
    up_votes = source.up_votes,
    down_votes = source.down_votes,
    views = source.views,
    website_url = source.website_url
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    user_id,
    display_name,
    about_me,
    age,
    creation_date,
    last_access_date,
    location,
    reputation,
    up_votes,
    down_votes,
    views,
    website_url
  ) VALUES (
    source.user_id,
    source.display_name,
    source.about_me,
    source.age,
    source.creation_date,
    source.last_access_date,
    source.location,
    source.reputation,
    source.up_votes,
    source.down_votes,
    source.views,
    source.website_url
  );
  --merge post_question
  MERGE `velvety-ring-349218.Stack_Overflow.posts_question` AS target

USING 
(SELECT
Generate_UUID() as sk, *
FROM `velvety-ring-349218.stackoverflow_raw.posts_question`)
AS source
ON
    target.posts_id = source.posts_id
WHEN MATCHED THEN
    UPDATE SET
        target.title = source.title,
        target.body = source.body,
        target.answer_count = source.answer_count,
        target.comment_count = source.comment_count,
        target.creation_date = source.creation_date,
        target.favorite_count = source.favorite_count,
        target.post_type_id = source.post_type_id,
        target.score = source.score,
        target.tags = source.tags,
        target.view_count = source.view_count
WHEN NOT MATCHED THEN
    INSERT ROW
        (posts_id, title, body, answer_count, comment_count, creation_date, favorite_count, post_type_id, score, tags, view_count)
    VALUES
        (source.posts_id, source.title, source.body, source.answer_count, source.comment_count, source.creation_date, source.favorite_count, source.post_type_id, source.score, source.tags, source.view_count);

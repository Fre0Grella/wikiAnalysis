# MediaWiki History Dumps Schema Index

Complete field index and documentation for the MediaWiki history dumps TSV format.

| Index | Field Name | Data Type | Description |
|-------|-----------|-----------|-------------|
| 0 | `wiki_db` | string | Wiki database name (e.g., enwiki, dewiki) |
| 1 | `event_entity` | string | Event type: revision, user, or page |
| 2 | `event_type` | string | Event subtype: create, move, delete, rename, altergroups, etc. |
| 3 | `event_timestamp` | string | When the event occurred |
| 4 | `event_comment` | string | Comment related to the event (from log, revision, or page) |
| 5 | `event_user_id` | bigint | ID of the user that caused the event (null if anonymous or deleted) |
| 6 | `event_user_text_historical` | string | Historical username or IP of the user that caused the event |
| 7 | `event_user_text` | string | Current username of the user (null for anonymous users) |
| 8 | `event_user_blocks_historical` | array<string> | Historical blocks of the event user |
| 9 | `event_user_blocks` | array<string> | Current blocks of the event user |
| 10 | `event_user_groups_historical` | array<string> | Historical groups of the event user |
| 11 | `event_user_groups` | array<string> | Current groups of the event user |
| 12 | `event_user_is_bot_by_historical` | array<string> | Historical bot status (values: name, group) |
| 13 | `event_user_is_bot_by` | array<string> | Current bot status (values: name, group) |
| 14 | `event_user_is_created_by_self` | boolean | Whether the user created their own account |
| 15 | `event_user_is_created_by_system` | boolean | Whether the account was created by MediaWiki system |
| 16 | `event_user_is_created_by_peer` | boolean | Whether the account was created by another user |
| 17 | `event_user_is_anonymous` | boolean | Whether the user is anonymous (old IP-based system) |
| 18 | `event_user_is_temporary` | boolean | Whether the user is temporary account (new system) |
| 19 | `event_user_is_permanent` | boolean | Whether the user is registered/permanent |
| 20 | `event_user_registration_timestamp` | string | User registration timestamp |
| 21 | `event_user_creation_timestamp` | string | User account creation timestamp (from logging table) |
| 22 | `event_user_first_edit_timestamp` | string | Timestamp of the user's first edit |
| 23 | `event_user_revision_count` | bigint | Number of revisions made by the event user up to this time |
| 24 | `event_user_seconds_since_previous_revision` | bigint | Seconds since the user's previous revision |
| 25 | `page_id` | bigint | Page ID (in revision/page events) |
| 26 | `page_title_historical` | string | Historical title of the page |
| 27 | `page_title` | string | Current title of the page |
| 28 | `page_namespace_historical` | int | Historical namespace of the page |
| 29 | `page_namespace_is_content_historical` | boolean | Whether historical namespace is categorized as content |
| 30 | `page_namespace` | int | Current namespace of the page |
| 31 | `page_namespace_is_content` | boolean | Whether current namespace is categorized as content |
| 32 | `page_is_redirect` | boolean | Whether the page is currently a redirect |
| 33 | `page_is_deleted` | boolean | Whether the page is rebuilt from a delete event |
| 34 | `page_creation_timestamp` | string | Creation timestamp of the page |
| 35 | `page_first_edit_timestamp` | string | Timestamp of the page's first revision |
| 36 | `page_revision_count` | bigint | Cumulative revision count for the page |
| 37 | `page_seconds_since_previous_revision` | bigint | Seconds since the page's previous revision |
| 38 | `user_id` | bigint | User ID (in user events only) |
| 39 | `user_text_historical` | string | Historical username or IP (in user events only) |
| 40 | `user_text` | string | Current username or IP (in user events only) |
| 41 | `user_blocks_historical` | array<string> | Historical blocks (in user events only) |
| 42 | `user_blocks` | array<string> | Current blocks (in user events only) |
| 43 | `user_groups_historical` | array<string> | Historical groups (in user events only) |
| 44 | `user_groups` | array<string> | Current groups (in user events only) |
| 45 | `user_is_bot_by_historical` | array<string> | Historical bot status (in user events only) |
| 46 | `user_is_bot_by` | array<string> | Current bot status (in user events only) |
| 47 | `user_is_created_by_self` | boolean | Whether user created their own account (user events only) |
| 48 | `user_is_created_by_system` | boolean | Whether account created by MediaWiki (user events only) |
| 49 | `user_is_created_by_peer` | boolean | Whether account created by another user (user events only) |
| 50 | `user_is_anonymous` | boolean | Whether user is anonymous (user events only) |
| 51 | `user_is_temporary` | boolean | Whether user is temporary account (user events only) |
| 52 | `user_is_permanent` | boolean | Whether user is registered (user events only) |
| 53 | `user_registration_timestamp` | string | User registration timestamp (user events only) |
| 54 | `user_creation_timestamp` | string | User account creation timestamp (user events only) |
| 55 | `user_first_edit_timestamp` | string | Timestamp of user's first edit (user events only) |
| 56 | `revision_id` | bigint | Revision ID (in revision events only) |
| 57 | `revision_parent_id` | bigint | Parent revision ID (in revision events only) |
| 58 | `revision_minor_edit` | boolean | Whether the revision is marked as minor |
| 59 | `revision_deleted_parts` | array<string> | Deleted parts (values: text, comment, user) |
| 60 | `revision_deleted_parts_are_suppressed` | boolean | Whether deleted parts are suppressed from admins |
| 61 | `revision_text_bytes` | bigint | Number of bytes in the revision |
| 62 | `revision_text_bytes_diff` | bigint | Change in bytes relative to parent revision |
| 63 | `revision_text_sha1` | string | SHA1 hash of the revision |
| 64 | `revision_content_model` | string | Content model of the revision |
| 65 | `revision_content_format` | string | Content format of the revision |
| 66 | `revision_is_deleted_by_page_deletion` | boolean | Whether revision was deleted with the page |
| 67 | `revision_deleted_by_page_deletion_timestamp` | string | Timestamp when revision was deleted |
| 68 | `revision_is_identity_reverted` | boolean | Whether the revision was reverted |
| 69 | `revision_first_identity_reverting_revision_id` | bigint | ID of the revision that reverted this one |
| 70 | `revision_seconds_to_identity_revert` | bigint | Seconds between revision and its revert |
| 71 | `revision_is_identity_revert` | boolean | Whether this revision reverts other revisions |
| 72 | `revision_is_from_before_page_creation` | boolean | Whether revision timestamp is before page creation |
| 73 | `revision_tags` | array<string> | Tags associated with the revision |

## Important Notes

- **Null/Empty Values:** Represented as empty fields in the TSV.
- **Array Format:** Arrays are encoded as `value1,value2,...` with commas escaped in values.
- **Field Prefixes:** All fields except `event_global` class are prefixed with their class name (e.g., `page_`, `revision_`, `user_`).
- **Denormalization:** Many fields are null/empty depending on the `event_entity`:
  - **revision events:** page fields, revision fields, and event_user fields are populated.
  - **page events:** page fields and event_user fields are populated (revision fields are null).
  - **user events:** user fields and event_user fields are populated (page and revision fields are null).

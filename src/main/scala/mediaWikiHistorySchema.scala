// Helper class defining the schema for MediaWiki history data in a map
// of field names to their respective indices.
class mediaWikiHistorySchema {

  private val Fields = Map(
    // Event_global
    "wiki_db"         -> 0,
    "event_entity"    -> 1,
    "event_type"      -> 2,
    "event_timestamp" -> 3,
    "event_comment"   -> 4,

    // Event user
    "event_user_id"                              -> 5,
    "event_user_central_id"                      -> 6,
    "event_user_text_historical"                 -> 7,
    "event_user_text"                            -> 8,
    "event_user_blocks_historical"               -> 9,
    "event_user_blocks"                          -> 10,
    "event_user_groups_historical"               -> 11,
    "event_user_groups"                          -> 12,
    "event_user_is_bot_by_historical"            -> 13,
    "event_user_is_bot_by"                       -> 14,
    "event_user_is_created_by_self"              -> 15,
    "event_user_is_created_by_system"            -> 16,
    "event_user_is_created_by_peer"              -> 17,
    "event_user_is_anonymous"                    -> 18,
    "event_user_is_temporary"                    -> 19,
    "event_user_is_permanent"                    -> 20,
    "event_user_registration_timestamp"          -> 21,
    "event_user_creation_timestamp"              -> 22,
    "event_user_first_edit_timestamp"            -> 23,
    "event_user_revision_count"                  -> 24,
    "event_user_seconds_since_previous_revision" -> 25,

    // Page
    "page_id"                              -> 26,
    "page_title_historical"                -> 27,
    "page_title"                           -> 28,
    "page_namespace_historical"            -> 29,
    "page_namespace_is_content_historical" -> 30,
    "page_namespace"                       -> 31,
    "page_namespace_is_content"            -> 32,
    "page_is_redirect"                     -> 33,
    "page_is_deleted"                      -> 34,
    "page_creation_timestamp"              -> 35,
    "page_first_edit_timestamp"            -> 36,
    "page_revision_count"                  -> 37,
    "page_seconds_since_previous_revision" -> 38,

    // User
    "user_id"                     -> 39,
    "user_central_id"             -> 40,
    "user_text_historical"        -> 41,
    "user_text"                   -> 42,
    "user_blocks_historical"      -> 43,
    "user_blocks"                 -> 44,
    "user_groups_historical"      -> 45,
    "user_groups"                 -> 46,
    "user_is_bot_by_historical"   -> 47,
    "user_is_bot_by"              -> 48,
    "user_is_created_by_self"     -> 49,
    "user_is_created_by_system"   -> 50,
    "user_is_created_by_peer"     -> 51,
    "user_is_anonymous"           -> 52,
    "user_is_temporary"           -> 53,
    "user_is_permanent"           -> 54,
    "user_registration_timestamp" -> 55,
    "user_creation_timestamp"     -> 56,
    "user_first_edit_timestamp"   -> 57,

    // Revision
    "revision_id"                                   -> 58,
    "revision_parent_id"                            -> 59,
    "revision_minor_edit"                           -> 60,
    "revision_deleted_parts"                        -> 61,
    "revision_deleted_parts_are_suppressed"         -> 62,
    "revision_text_bytes"                           -> 63,
    "revision_text_bytes_diff"                      -> 64,
    "revision_text_sha1"                            -> 65,
    "revision_content_model"                        -> 66,
    "revision_content_format"                       -> 67,
    "revision_is_deleted_by_page_deletion"          -> 68,
    "revision_deleted_by_page_deletion_timestamp"   -> 69,
    "revision_is_identity_reverted"                 -> 70,
    "revision_first_identity_reverting_revision_id" -> 71,
    "revision_seconds_to_identity_revert"           -> 72,
    "revision_is_identity_revert"                   -> 73,
    "revision_is_from_before_page_creation"         -> 74,
    "revision_tags"                                 -> 75
  )

  def idx(name: String): Int = Fields.getOrElse(name, -1)
}

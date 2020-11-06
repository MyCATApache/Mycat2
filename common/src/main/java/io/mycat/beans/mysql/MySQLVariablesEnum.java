/**
 * Copyright (C) <2020>  <chenjunwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql;

/**
 * @author: chenjunwen 294712221
 */
public enum MySQLVariablesEnum {
    //    autocommit(Type.BOTH),
//    sql_mode(Type.BOTH),
//    time_zone(Type.BOTH),
//    tx_isolation(Type.BOTH),
//    max_execution_time(Type.BOTH),
//    innodb_lock_wait_timeout(Type.BOTH),
    performance_schema(Type.GLOBAL),
    version_comment(Type.GLOBAL),
    version(Type.GLOBAL),
    license(Type.GLOBAL),
    lower_case_table_names(Type.GLOBAL),
    system_time_zone(Type.GLOBAL),
    // 注意:不包含 new		Both
    audit_log_connection_policy(Type.GLOBAL),
    audit_log_exclude_accounts(Type.GLOBAL),
    audit_log_flush((Type.GLOBAL)),
    audit_log_include_accounts(Type.GLOBAL),
    audit_log_read_buffer_size(Type.VARIES),
    audit_log_rotate_on_size(Type.GLOBAL),
    audit_log_statement_policy(Type.GLOBAL),
    authentication_Idap_sasl_auth_method_name(Type.GLOBAL),
    authentication_ldap_sasl_bind_base_dn(Type.GLOBAL),
    authentication_Idap_sasl_bind_root_dn((Type.GLOBAL)),
    authentication_Idap_sasl_bind_root_pwd((Type.GLOBAL)),
    authentication_Idap_sasl_ca_path(Type.GLOBAL),
    authentication_Idap_sasl_group_search_attr(Type.GLOBAL),
    authentication_Idap_sasl_group_search_filter(Type.GLOBAL),
    authentication_ldap_sasl_init_pool_size(Type.GLOBAL),
    authentication_Idap_sasl_log_status(Type.GLOBAL),
    authentication_Idap_sasl_max_pool_size(Type.GLOBAL),
    authentication_Idap_sasl_server_host(Type.GLOBAL),
    authentication_Idap_sasl_server_port(Type.GLOBAL),
    authentication_Idap_sasl_tls(Type.GLOBAL),
    authentication_Idap_sasl_user_search_attr(Type.GLOBAL),
    authentication_ldap_simple_auth_method_name(Type.GLOBAL),
    authentication_Idap_simple_bind_base_dn(Type.GLOBAL),
    authentication_Idap_simple_bind_root_dn(Type.GLOBAL),
    authentication_Idap_simple_bind_root_pwd(Type.GLOBAL),
    authentication_ldap_simple_ca_path(Type.GLOBAL),
    authentication_Idap_simple_group_search_attr(Type.GLOBAL),
    authentication_Idap_simple(Type.GLOBAL),

    group_search_filter(Type.GLOBAL),

    authentication_Idap_simple_init_pool_size(Type.GLOBAL),

    authentication_Idap_simple_log_status(Type.GLOBAL),

    authentication_Idap_simple_max_pool_size(Type.GLOBAL),

    authentication_Idap_simple_server_host(Type.GLOBAL),

    authentication_Idap_simple_server_port(Type.GLOBAL),

    authentication_Idap_simple_tls(Type.GLOBAL),

    authentication_Idap_simple_user_search_attr(Type.GLOBAL),

    auto_increment_increment(Type.BOTH),

    auto_increment_offset(Type.BOTH),

    autocommit(Type.BOTH),

    automatic_sp_privileges(Type.GLOBAL),

    avoid_temporal_upgrade(Type.GLOBAL),

    big_tables(Type.BOTH),

    binlog_cache_size(Type.GLOBAL),

    binlog_checksum(Type.GLOBAL),

    binlog_direct_non_transactional_updates(Type.BOTH),

    binlog_error_action(Type.GLOBAL),

    binlog_format(Type.BOTH),

    binlog_group_commit_sync_delay(Type.GLOBAL),

    binlog_group_commit_sync_no_delay_count(Type.GLOBAL),

    binlog_max_flush_queue_time(Type.GLOBAL),

    binlog_order_commits(Type.GLOBAL),

    binlog_row_image(Type.BOTH),

    binlog_rows_query_log_events(Type.BOTH),

    binlog_stmt_cache_size(Type.GLOBAL),

    binlog_transaction_dependency_history_size(Type.GLOBAL),

    binlog_transaction_dependency_tracking(Type.GLOBAL),

    block_encryption_mode(Type.BOTH),

    bulk_insert_buffer_size(Type.BOTH),

    character_set_client(Type.BOTH),

    character_set_connection(Type.BOTH),

    character_set_database(Type.BOTH),

    character_set_filesystem(Type.BOTH),

    character_set_results(Type.BOTH),

    character_set_server(Type.BOTH),

    check_proxy_users(Type.GLOBAL),

    collation_connection(Type.BOTH),

    collation_database(Type.BOTH),

    collation_server(Type.BOTH),
    completion_type(Type.BOTH),
    concurrent_insert(Type.GLOBAL),
    connect(Type.GLOBAL),

    timeout(Type.GLOBAL),

    connection_control_failed_connections_threshold(Type.GLOBAL),

    connection_control_max_connection_delay(Type.GLOBAL),

    connection_control_min_connection_delay(Type.GLOBAL),

    debug(Type.BOTH),

    debug_sync(Type.SESSION),

    default_password_lifetime(Type.GLOBAL),

    default_storage_engine(Type.BOTH),

    default_tmp_storage_engine(Type.BOTH),

    default_week_format(Type.BOTH),

    delay_key_write(Type.GLOBAL),

    delayed_insert_limit(Type.GLOBAL),

    delayed_insert_timeout(Type.GLOBAL),

    delayed_queue_size(Type.GLOBAL),

    div_precision_increment(Type.BOTH),

    end_markers_in_json(Type.BOTH),

    enforce_gtid_consistency(Type.GLOBAL),

    eq_range_index_dive_limit(Type.BOTH),

    event_scheduler(Type.GLOBAL),

    expire_logs_days(Type.GLOBAL),

    explicit_defaults_for_timestamp(Type.BOTH),

    flush(Type.BOTH),

    flush_time(Type.GLOBAL),

    foreign_key_checks(Type.BOTH),

    ft_boolean_syntax(Type.GLOBAL),

    general_log(Type.GLOBAL),

    general_log_file(Type.GLOBAL),

    group_concat_max_len(Type.BOTH),

    group_replication_allow_local_disjoint_gtids_join(Type.GLOBAL),

    group_replication_allow_local_lower_version_join(Type.GLOBAL),

    group_replication_auto_increment_increment(Type.GLOBAL),

    group_replication_bootstrap_group(Type.GLOBAL),

    group_replication_components_stop_timeout(Type.GLOBAL),

    group_replication_compression_threshold(Type.GLOBAL),

    group_replication_enforce_update_everywhere_checks(Type.GLOBAL),

    group_replication_exit_state_action(Type.GLOBAL),

    group_replication_flow_control_certifier_threshold(Type.GLOBAL),
    group_replication_flow_control_mode(Type.GLOBAL),
    group_replication_force_members(Type.GLOBAL),
    group_replication_group_name(Type.GLOBAL),
    group_replication_group_seeds(Type.GLOBAL),
    group_replication_gtid_assignment_block_size(Type.GLOBAL),
    group_replication_ip_whitelist(Type.GLOBAL),
    group_replication_local_address(Type.GLOBAL),
    group_replication_member_weight(Type.GLOBAL),
    group_replication_poll_spin_loops(Type.GLOBAL),
    group_replication_recovery_complete_at(Type.GLOBAL),
    group_replication_recovery_reconnect_interval(Type.GLOBAL),
    group_replication_recovery_retry_count(Type.GLOBAL),
    group_replication_recovery_ssl_ca(Type.GLOBAL),
    group_replication_recovery_ssl_capath(Type.GLOBAL),
    group_replication_recovery_ssl_cert(Type.GLOBAL),
    group_replication_recovery_ssl_cipher(Type.GLOBAL),
    group_replication_recovery_ssl_crl(Type.GLOBAL),
    group_replication_recovery_ssl_crlpath(Type.GLOBAL),
    group_replication_recovery_ssl_key(Type.GLOBAL),
    group_replication_recovery_ssl_verify_server_cert(Type.GLOBAL),
    group_replication_recovery_use_ssl(Type.GLOBAL),
    group_replication_single_primary_mode(Type.GLOBAL),
    group_replication_ssl_mode(Type.GLOBAL),
    group_replication_start_on_boot(Type.GLOBAL),
    group_replication_transaction_size_limit(Type.GLOBAL),
    group_replication_unreachable_majority_timeout(Type.GLOBAL),
    gtid_executed_compression_period(Type.GLOBAL),
    gtid_mode(Type.GLOBAL),
    gtid_next(Type.GLOBAL),
    gtid_purged(Type.SESSION),
    host_cache_size(Type.GLOBAL),
    identity(Type.SESSION),
    init_connect(Type.GLOBAL),
    init_slave(Type.GLOBAL),
    innodb_adaptive_flushing(Type.GLOBAL),
    innodb_adaptive_flushing_lwm(Type.GLOBAL),
    innodb_adaptive_hash_index(Type.GLOBAL),
    innodb_adaptive_max_sleep_delay(Type.GLOBAL),
    innodb_api_bk_commit_interval(Type.GLOBAL),
    innodb_api_trx_level(Type.GLOBAL),
    innodb_autoextend_increment(Type.GLOBAL),
    innodb_background_drop_list_empty(Type.GLOBAL),
    innodb_buffer_pool_dump_at_shutdown(Type.GLOBAL),
    innodb_buffer_pool_dump_now(Type.GLOBAL),
    innodb_buffer_pool_dump_pct(Type.GLOBAL),
    innodb_buffer_pool_filename(Type.GLOBAL),
    innodb_buffer_pool_load_abort(Type.GLOBAL),
    innodb_buffer_pool_load_now(Type.GLOBAL),
    innodb_buffer_pool_size(Type.GLOBAL),
    innodb_change_buffer_max_size(Type.GLOBAL),
    innodb_change_buffering(Type.GLOBAL),
    innodb_change_buffering_debug(Type.GLOBAL),
    innodb_checksum_algorithm(Type.GLOBAL),
    innodb_cmp_per_index_enabled(Type.GLOBAL),
    innodb_commit_concurrency(Type.GLOBAL),
    innodb_compress_debug(Type.GLOBAL),
    innodb_compression_failure_threshold_pct(Type.GLOBAL),
    innodb_compression_level(Type.GLOBAL),
    innodb_compression_pad_pct_max(Type.GLOBAL),
    innodb_concurrency_tickets(Type.GLOBAL),
    innodb_deadlock_detect(Type.GLOBAL),
    innodb_default_row_format(Type.GLOBAL),
    innodb_disable_resize_buffer_pool_debug(Type.GLOBAL),
    innodb_disable_sort_file_cache(Type.GLOBAL),
    innodb_fast_shutdown(Type.GLOBAL),
    innodb_fil_make_page_dirty_debug(Type.GLOBAL),
    innodb_file_format(Type.GLOBAL),
    innodb_file_format_max(Type.GLOBAL),
    innodb_file_per_table(Type.GLOBAL),
    innodb_fill_factor(Type.GLOBAL),
    innodb_flush_log_at_timeout(Type.GLOBAL),
    innodb_flush_log_at_trx_commit(Type.GLOBAL),
    innodb_flush_neighbors(Type.GLOBAL),
    innodb_flush_sync(Type.GLOBAL),
    innodb_flushing_avg_loops(Type.GLOBAL),
    innodb_ft_aux_table(Type.GLOBAL),
    innodb_ft_enable_diag_print(Type.GLOBAL),
    innodb_ft_enable_stopword(Type.BOTH),
    innodb_ft_num_word_optimize(Type.GLOBAL),
    innodb_ft_result_cache_limit(Type.GLOBAL),
    innodb_ft_server_stopword_table(Type.GLOBAL),
    innodb_ft_user_stopword_table(Type.GLOBAL),
    innodb_io_capacity(Type.BOTH),
    innodb_io_capacity_max(Type.GLOBAL),
    innodb_large_prefix(Type.GLOBAL),
    innodb_limit_optimistic_insert_debug(Type.GLOBAL),
    innodb_lock_wait_timeout(Type.BOTH),
    innodb_log_checkpoint_now(Type.GLOBAL),
    innodb_log_checksums(Type.GLOBAL),
    innodb_log_compressed_pages(Type.GLOBAL),
    innodb_log_write_ahead_size(Type.GLOBAL),
    innodb_lru_scan_depth(Type.GLOBAL),
    innodb_max_dirty_pages_pct(Type.GLOBAL),
    innodb_max_dirty_pages_pct_lwm(Type.GLOBAL),

    Numeric(Type.GLOBAL),

    innodb_max_purge_lag(Type.GLOBAL),

    innodb_max_purge_lag_delay(Type.GLOBAL),

    innodb_max_undo_log_size(Type.GLOBAL),

    innodb_merge_threshold_set_all_debug(Type.GLOBAL),

    innodb_monitor_disable(Type.GLOBAL),

    innodb_monitor_enable(Type.GLOBAL),

    innodb_monitor_reset(Type.GLOBAL),

    innodb_monitor_reset_all(Type.GLOBAL),

    innodb_old_blocks_pct(Type.GLOBAL),

    innodb_old_blocks_time(Type.GLOBAL),

    innodb_online_alter_log_max_size(Type.GLOBAL),

    innodb_optimize_fulltext_only(Type.GLOBAL),

    innodb_print_all_deadlocks(Type.GLOBAL),

    innodb_purge_batch_size(Type.GLOBAL),

    innodb_purge_rseg_truncate_frequency(Type.GLOBAL),

    innodb_random_read_ahead(Type.GLOBAL),

    innodb_read_ahead_threshold(Type.GLOBAL),

    innodb_replication_delay(Type.GLOBAL),

    innodb_rollback_segments(Type.GLOBAL),

    innodb_saved_page_number_debug(Type.GLOBAL),

    innodb_spin_wait_delay(Type.GLOBAL),

    innodb_stats_auto_recalc(Type.GLOBAL),

    innodb_stats_include_delete_marked(Type.GLOBAL),

    innodb_stats_method(Type.GLOBAL),

    innodb_stats_on_metadata(Type.GLOBAL),

    innodb_stats_persistent(Type.GLOBAL),

    innodb_stats_persistent_sample_pages(Type.GLOBAL),

    innodb_stats_sample_pages(Type.GLOBAL),

    innodb_stats_transient_sample_pages(Type.GLOBAL),

    innodb_status_output(Type.GLOBAL),

    innodb_status_output_locks(Type.GLOBAL),

    innodb_strict_mode(Type.BOTH),

    innodb_support_xa(Type.BOTH),

    innodb_sync_spin_loops(Type.GLOBAL),

    innodb_table_locks(Type.GLOBAL),

    innodb_thread_concurrency(Type.BOTH),

    innodb_thread_sleep_delay(Type.GLOBAL),

    innodb_tmpdir(Type.GLOBAL),

    innodb_trx_purge_view_update_only_debug(Type.BOTH),

    innodb_trx_rseg_n_slots_debug(Type.BOTH),

    innodb_undo_log_truncate(Type.GLOBAL),

    innodb_undo_logs(Type.GLOBAL),

    insert_id(Type.GLOBAL),

    interactive_timeout(Type.SESSION),

    internal_tmp_disk_storage_engine(Type.GLOBAL),

    join_buffer_size(Type.BOTH),

    keep_files_on_create(Type.BOTH),

    key_buffer_size(Type.GLOBAL),

    key_cache_age_threshold(Type.GLOBAL),

    key_cache_block_size(Type.GLOBAL),

    key_cache_division_limit(Type.GLOBAL),

    keyring_aws_cmk_id(Type.GLOBAL),

    keyring_aws_region(Type.GLOBAL),

    keyring_encrypted_file_data(Type.GLOBAL),

    keyring_encrypted_file_password(Type.GLOBAL),

    keyring_file_data(Type.GLOBAL),

    keyring_okv_conf_dir(Type.GLOBAL),

    keyring_operations(Type.GLOBAL),

    last_insert_id(Type.SESSION),

    lc_messages(Type.BOTH),

    lc_time_names(Type.BOTH),

    local_infile(Type.GLOBAL),

    lock_wait_timeout(Type.BOTH),

    log_bin_trust_function_creators(Type.GLOBAL),

    log_builtin_as_identified_by_password(Type.GLOBAL),

    log_error_verbosity(Type.GLOBAL),

    log_output(Type.GLOBAL),

    log_queries_not_using_indexes(Type.GLOBAL),

    log_slow_admin_statements(Type.GLOBAL),

    log_slow_slave_statements(Type.GLOBAL),

    log_statements_unsafe_for_binlog(Type.GLOBAL),

    log_syslog(Type.GLOBAL),

    log_syslog_facility(Type.GLOBAL),

    log_syslog_include_pid(Type.GLOBAL),

    log_syslog_tag(Type.GLOBAL),

    log_throttle_queries_not_using_indexes(Type.GLOBAL),

    log_timestamps(Type.GLOBAL),

    log_warnings(Type.GLOBAL),

    long_query_time(Type.BOTH),

    low_priority_updates(Type.BOTH),

    master_info_repository(Type.GLOBAL),

    master_verify_checksum(Type.GLOBAL),

    max_allowed_packet(Type.BOTH),

    max_binlog_cache_size(Type.GLOBAL),

    max_binlog_size(Type.GLOBAL),

    max_binlog_stmt_cache_size(Type.GLOBAL),

    max_connect_errors(Type.GLOBAL),

    max_connections(Type.GLOBAL),

    max_delayed_threads(Type.BOTH),

    max_error_count(Type.BOTH),

    max_execution_time(Type.BOTH),

    max_heap_table_size(Type.BOTH),

    max_insert_delayed_threads(Type.BOTH),

    max_join_size(Type.BOTH),

    max_length_for_sort_data(Type.BOTH),

    max_points_in_geometry(Type.BOTH),

    max_prepared_stmt_count(Type.GLOBAL),

    max_relay_log_size(Type.BOTH),

    max_seeks_for_key(Type.BOTH),

    max_sort_length(Type.BOTH),

    max_sp_recursion_depth(Type.BOTH),

    max_tmp_tables(Type.BOTH),

    max_user_connections(Type.BOTH),

    max_write_lock_count(Type.GLOBAL),

    min_examined_row_limit(Type.BOTH),

    multi_range_count(Type.BOTH),

    myisam_data_pointer_size(Type.GLOBAL),

    myisam_max_sort_file_size(Type.GLOBAL),

    myisam_repair_threads(Type.BOTH),

    myisam_sort_buffer_size(Type.BOTH),

    myisam_stats_method(Type.BOTH),

    myisam_use_mmap(Type.GLOBAL),

    mysql_firewall_mode(Type.GLOBAL),

    mysql_firewall_trace(Type.GLOBAL),

    mysql_native_password_proxy_users(Type.GLOBAL),

    mysqlx_connect_timeout(Type.GLOBAL),

    mysqlx_idle_worker_thread_timeout(Type.GLOBAL),

    mysqlx_max_allowed_packet(Type.GLOBAL),

    mysqlx_max_connections(Type.GLOBAL),

    mysqlx_min_worker_threads(Type.GLOBAL),

    ndb_allow_copying_alter_table(Type.BOTH),

    ndb_autoincrement_prefetch_sz(Type.BOTH),

    ndb_blob_read_batch_bytes(Type.BOTH),

    ndb_blob_write_batch_bytes(Type.BOTH),

    ndb_cache_check_time(Type.GLOBAL),

    ndb_clear_apply_status(Type.GLOBAL),

    ndb_data_node_neighbour(Type.GLOBAL),

    ndb_default_column_format(Type.GLOBAL),


    ndb_deferred_constraints(Type.BOTH),


    ndb_distribution(Type.GLOBAL),

    ndb_eventbuffer_free_percent(Type.GLOBAL),

    ndb_eventbuffer_max_alloc(Type.GLOBAL),

    ndb_extra_logging(Type.GLOBAL),

    ndb_force_send(Type.BOTH),

    ndb_fully_replicated(Type.BOTH),

    ndb_index_stat_enable(Type.BOTH),

    ndb_index_stat_option(Type.BOTH),

    ndb_join_pushdown(Type.BOTH),

    ndb_log_bin(Type.BOTH),

    ndb_log_binlog_index(Type.GLOBAL),

    ndb_log_empty_epochs(Type.GLOBAL),

    ndb_log_empty_update(Type.GLOBAL),

    ndb_log_exclusive_reads(Type.BOTH),

    ndb_log_update_as_write(Type.GLOBAL),

    ndb_log_update_minimal(Type.GLOBAL),

    ndb_log_updated_only(Type.GLOBAL),

    ndb_optimization_delay(Type.GLOBAL),

    ndb_read_backup(Type.GLOBAL),

    ndb_recv_thread_activation_threshold(Type.GLOBAL),

    ndb_recv_thread_cpu_mask(Type.GLOBAL),

    ndb_report_thresh_binlog_epoch_slip(Type.GLOBAL),

    ndb_report_thresh_binlog_mem_usage(Type.GLOBAL),

    ndb_row_checksum(Type.BOTH),

    ndb_show_foreign_key_mock_tables(Type.GLOBAL),

    ndb_slave_conflict_role(Type.GLOBAL),

    ndb_table_no_logging(Type.SESSION),

    ndb_table_temporary(Type.SESSION),

    ndb_use_exact_count(Type.BOTH),

    ndb_use_transactions(Type.BOTH),

    ndbinfo_max_bytes(Type.BOTH),

    ndbinfo_max_rows(Type.BOTH),

    ndbinfo_offline(Type.GLOBAL),

    ndbinfo_show_hidden(Type.BOTH),

    ndbinfo_table_prefix(Type.BOTH),

    net_buffer_length(Type.BOTH),

    net_read_timeout(Type.BOTH),

    net_retry_count(Type.BOTH),

    net_write_timeout(Type.BOTH),

    //new		Both
    offline_mode(Type.GLOBAL),

    old_alter_table(Type.BOTH),

    old_passwords(Type.BOTH),

    optimizer_prune_level(Type.BOTH),

    optimizer_search_depth(Type.BOTH),

    optimizer_switch(Type.BOTH),

    optimizer_trace(Type.BOTH),

    optimizer_trace_features(Type.BOTH),

    optimizer_trace_limit(Type.BOTH),

    optimizer_trace_max_mem_size(Type.BOTH),

    optimizer_trace_offset(Type.BOTH),

    parser_max_mem_size(Type.BOTH),

    preload_buffer_size(Type.BOTH),

    profiling(Type.BOTH),

    profiling_history_size(Type.BOTH),

    pseudo_slave_mode(Type.SESSION),

    pseudo_thread_id(Type.SESSION),

    query_alloc_block_size(Type.BOTH),

    query_cache_limit(Type.GLOBAL),

    query_cache_min_res_unit(Type.GLOBAL),

    query_cache_size(Type.GLOBAL),

    query_cache_type(Type.BOTH),

    query_cache_wlock_invalidate(Type.BOTH),

    query_prealloc_size(Type.BOTH),

    rand_seed1(Type.SESSION),

    rand_seed2(Type.SESSION),

    range_alloc_block_size(Type.BOTH),

    range_optimizer_max_mem_size(Type.BOTH),

    rbr_exec_mode(Type.BOTH),

    read_buffer_size(Type.BOTH),

    read_only(Type.GLOBAL),

    read_rnd_buffer_size(Type.BOTH),

    relay_log_info_repository(Type.GLOBAL),

    relay_log_purge(Type.GLOBAL),

    require_secure_transport(Type.GLOBAL),

    rewriter_enabled(Type.GLOBAL),

    rewriter_verbose(Type.GLOBAL),

    rpl_semi_sync_master_enabled(Type.GLOBAL),

    rpl_semi_sync_master_timeout(Type.GLOBAL),

    rpl_semi_sync_master_trace_level(Type.GLOBAL),

    rpl_semi_sync_master_wait_for_slave_count(Type.GLOBAL),

    rpl_semi_sync_master_wait_no_slave(Type.GLOBAL),

    rpl_semi_sync_master_wait_point(Type.GLOBAL),

    rpl_semi_sync_slave_enabled(Type.GLOBAL),

    rpl_semi_sync_slave_trace_level(Type.GLOBAL),

    rpl_stop_slave_timeout(Type.GLOBAL),

    secure_auth(Type.GLOBAL),

    server_id(Type.GLOBAL),

    session_track_gtids(Type.BOTH),

    session_track_schema(Type.BOTH),

    session_track_state_change(Type.BOTH),

    session_track_system_variables(Type.BOTH),

    session_track_transaction_info(Type.BOTH),

    sha256_password_proxy_users(Type.GLOBAL),

    show_compatibility_56(Type.GLOBAL),

    show_create_table_verbosity(Type.BOTH),

    show_old_temporals(Type.BOTH),

    slave_allow_batching(Type.GLOBAL),

    slave_checkpoint_group(Type.GLOBAL),

    slave_checkpoint_period(Type.GLOBAL),

    slave_compressed_protocol(Type.GLOBAL),

    slave_exec_mode(Type.GLOBAL),

    slave_max_allowed_packet(Type.GLOBAL),

    slave_net_timeout(Type.GLOBAL),

    slave_parallel_type(Type.GLOBAL),

    slave_parallel_workers(Type.GLOBAL),

    slave_pending_jobs_size_max(Type.GLOBAL),

    slave_preserve_commit_order(Type.GLOBAL),

    slave_rows_search_algorithms(Type.GLOBAL),

    slave_sql_verify_checksum(Type.GLOBAL),

    slave_transaction_retries(Type.GLOBAL),

    slow_launch_time(Type.GLOBAL),

    slow_query_log(Type.GLOBAL),

    slow_query_log_file(Type.GLOBAL),

    sort_buffer_size(Type.GLOBAL),

    sql_auto_is_null(Type.BOTH),

    sql_big_selects(Type.BOTH),

    sql_buffer_result(Type.BOTH),

    sql_log_bin(Type.SESSION),

    sql_log_off(Type.BOTH),

    sql_mode(Type.BOTH),

    sql_notes(Type.BOTH),

    sql_quote_show_create(Type.BOTH),

    sql_safe_updates(Type.BOTH),

    sql_select_limit(Type.BOTH),

    sql_slave_skip_counter(Type.GLOBAL),

    sql_warnings(Type.BOTH),

    stored_program_cache(Type.GLOBAL),

    super_read_only(Type.GLOBAL),

    sync_binlog(Type.GLOBAL),

    sync_frm(Type.GLOBAL),

    sync_master_info(Type.GLOBAL),

    sync_relay_log(Type.GLOBAL),

    sync_relay_log_info(Type.GLOBAL),

    table_definition_cache(Type.GLOBAL),

    table_open_cache(Type.GLOBAL),

    thread_cache_size(Type.GLOBAL),

    thread_pool_high_priority_connection(Type.BOTH),

    thread_pool_max_unused_threads(Type.GLOBAL),

    thread_pool_prio_kickup_timer(Type.BOTH),

    thread_pool_stall_limit(Type.GLOBAL),

    time_zone(Type.BOTH),

    timestamp(Type.SESSION),

    tmp_table_size(Type.BOTH),

    transaction_alloc_block_size(Type.BOTH),

    transaction_allow_batching(Type.SESSION),

    transaction_isolation(Type.BOTH),

    transaction_prealloc_size(Type.BOTH),

    transaction_read_only(Type.BOTH),

    transaction_write_set_extraction(Type.BOTH),

    tx_isolation(Type.BOTH),

    tx_read_only(Type.BOTH),

    unique_checks(Type.BOTH),

    updatable_views_with_limit(Type.BOTH),

    validate_password_check_user_name(Type.GLOBAL),

    validate_password_dictionary_file(Type.GLOBAL),

    validate_password_length(Type.GLOBAL),

    validate_password_mixed_case_count(Type.GLOBAL),

    validate_password_number_count(Type.GLOBAL),

    validate_password_policy(Type.GLOBAL),

    validate_password_special_char_count(Type.GLOBAL),

    version_tokens_session(Type.BOTH),

    wait_timeout(Type.BOTH),
    ;

    Type type;

    MySQLVariablesEnum(Type type) {
        this.type = type;
    }

    public String[] getColumnNames() {
        switch (this.type) {
            case VARIES:
            case SESSION:
            case BOTH:
                return new String[]{"@@session." + this.name(), "@@global." + this.name()};
            case GLOBAL:

                return new String[]{"@@global." + this.name()};
            default:
                throw new UnsupportedOperationException();
        }
    }

    public String getColumnName() {
        switch (this.type) {
            case VARIES:
            case SESSION:
            case BOTH:
                return "@@session." + this.name();
            case GLOBAL:

                return "@@global." + this.name();
            default:
                throw new UnsupportedOperationException();
        }
    }

   static public MySQLVariablesEnum parseFromColumnName(String text) {
        String sessionPre = "@@session.";
         text = text.toLowerCase();
        if (text.startsWith(sessionPre)) {
            return MySQLVariablesEnum.valueOf(text.substring(sessionPre.length()));
        }
        String globalPre = "@@global.";
        if (text.startsWith(globalPre)) {
            return MySQLVariablesEnum.valueOf(text.substring(globalPre.length()));
        }
        if (text.startsWith("@@")){
            return MySQLVariablesEnum.valueOf(text.substring("@@".length()));
        }
        return null;
    }

    public enum Type {
        BOTH,
        GLOBAL,
        SESSION,
        VARIES
    }
}
<?php

namespace TractorCow\Fluent\Task;

use SilverStripe\Core\ClassInfo;
use SilverStripe\Core\Convert;
use SilverStripe\Core\Environment;
use SilverStripe\Dev\BuildTask;
use SilverStripe\i18n\i18n;
use SilverStripe\ORM\Connect\DatabaseException;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DB;
use SilverStripe\ORM\Queries\SQLSelect;
use SilverStripe\Security\DefaultAdminService;
use SilverStripe\Security\Member;
use SilverStripe\Versioned\Versioned;
use TractorCow\Fluent\Extension\FluentExtension;
use TractorCow\Fluent\Extension\FluentFilteredExtension;
use TractorCow\Fluent\Model\Locale;
use TractorCow\Fluent\State\FluentState;
use TractorCow\Fluent\Task\ConvertTranslatableTask\Exception;

/**
 * Provides migration from the Translatable module in a SilverStripe 3 website to the Fluent format for SilverStripe 4.
 * This task assumes that you have upgraded your website to run on SilverStripe 4 already, and you want to migrate the
 * existing data from your project into a format that is compatible with Fluent.
 *
 * Don't forget to:
 *
 * 1. Back up your DB
 * 2. dev/build
 * 3. Log into the CMS and set up the locales you want to use
 * 4. Back up your DB again
 * 5. Log into the CMS and check everything
 */
class ConvertTranslatableTask extends BuildTask
{
    protected $title = "Convert Translatable > Fluent Task";

    protected $description = "Migrates site DB from SS3 Translatable DB format to SS4 Fluent.";

    private static $segment = 'ConvertTranslatableTask';

    private $default_locale = 'en_US';

    private $base_table = 'SiteTree';

    private $line_break = "\r\n";

    public function run($request)
    {
        if (!Environment::isCli()) {
            $this->line_break = '<br>';
        }

        $this->checkInstalled();

        // we may need some privileges for this to work
        // without this, running under sake is a problem
        // maybe sake could take care of it...
        Member::actAs(
            DefaultAdminService::singleton()->findOrCreateDefaultAdmin(),
            function () {
                DB::get_conn()->withTransaction(
                    function () {
                        $this->doTranslatableMigration();
                    }
                );
            }
        );
    }

    /**
     * Get current base table
     *
     * @param String $base_table
     * @return void
     */
    private function setDefaultLocale(String $default_locale)
    {
        $this->default_locale = $default_locale;
    }

    /**
     * Set base table
     *
     * @return void
     */
    private function getDefaultLocale(): String
    {
        return $this->default_locale ?? 'en_US';
    }

    /**
     * Get current base table
     *
     * @param String $base_table
     * @return void
     */
    private function setBaseTable(String $base_table)
    {
        $this->base_table = $base_table;
    }

    /**
     * Set base table
     *
     * @return void
     */
    private function getBaseTable(): String
    {
        return $this->base_table ?? 'SiteTree';
    }

    /**
     * Checks that fluent is configured correctly
     *
     * @throws ConvertTranslatableTask\Exception
     */
    private function checkInstalled()
    {
        // Assert that fluent is configured
        if (empty(Locale::getLocales())) {
            throw new Exception(
                "Please configure Fluent locales (in the CMS) prior to migrating from translatable"
            );
        }

        if (empty(Locale::getDefault())) {
            throw new Exception(
                "Please configure a Fluent default locale (in the CMS) prior to migrating from translatable"
            );
        }
    }

    /**
     * Do Translatable migration to Fluent
     *
     * @return void
     */
    private function doTranslatableMigration()
    {
        $this->setDefaultLocale(i18n::config()->get('default_locale'));
        Versioned::set_stage(Versioned::DRAFT);

        $fluent_classes = $this->getFluentClasses();
        if (count($fluent_classes) === 0) {
            echo "No classes have Fluent enabled, so skipping. {$this->line_break}";
        } else {
            echo "Fluent classes:{$this->line_break}";
            foreach ($fluent_classes as $fluent_class) {
                echo " - '{$fluent_class}'{$this->line_break}";
            }
        }
        echo "{$this->line_break}";

        $all_db_tables = DB::get_schema()->tableList();

        $translation_groups = [];

        $fluent_tables_for_versions_data_migration = [];

        foreach ($fluent_classes as $fluent_class) {
            $sitetree_records['deletion_exceptions'] = [];
            /* @var DataObject $fluent_class */
            // Ensure that a translationgroup table exists for this class
            $base_table = DataObject::getSchema()->baseDataTable($fluent_class);

            echo "Base table set to {$base_table}{$this->line_break}";
            echo "{$this->line_break}";

            // Set translation group table name if it exist else continue to next fluent class
            $translation_group_table = strtolower($base_table . "_translationgroups");
            if (isset($all_db_tables[$translation_group_table])) {
                $translation_group_table = $all_db_tables[$translation_group_table];
            } else {
                echo "Ignoring '${fluent_class}': No _translationgroups table{$this->line_break}";
                echo "{$this->line_break}";
                continue;
            }

            // Disable filter if it has been applied to the fluent class
            if (
                singleton($fluent_class)->hasMethod('has_extension')
                && $fluent_class::has_extension(FluentFilteredExtension::class)
            ) {
                $fluent_class::remove_extension(FluentFilteredExtension::class);
            }

            $select_all_translationgroup_ids_matching_current_fluent_class = "
                SELECT DISTINCT TranslationGroupID
                FROM {$translation_group_table}
                INNER JOIN {$base_table}
                ON {$translation_group_table}.OriginalID = {$base_table}.ID
                WHERE {$base_table}.ClassName = %s
                ORDER BY TranslationGroupID ASC
            ";

            $translation_group = DB::query(
                sprintf(
                    $select_all_translationgroup_ids_matching_current_fluent_class,
                    Convert::raw2sql($fluent_class, true)
                )
            );

            // Get all of unique translation group ids
            $translation_groups[] = [
                'base_table' => $base_table,
                'fluent_class' => $fluent_class,
                'translation_group_ids' => $translation_group->column(),
                'translation_group_table' => $translation_group_table,
            ];

            $fluent_tables_for_versions_data_migration[] = $fluent_class;
        }

        // Migrate versoin data before doing sync
        $this->copyVersionedDataToLocalisedVersionTable($fluent_classes);

        // Migrate translations in this group and store ids of SiteTree records to not delete
        $sitetree_records = $this->migrateTranslationGroups($translation_groups);

        // Delete all records in SiteTree that have locale not matching default locale or specified ids
        $this->deleteOldBaseItemsFromTables($sitetree_records);

        // Finally, clean up Translatable tables
        $this->cleanTranslatableTables();

        echo "End of translation migration{$this->line_break}";
        echo "{$this->line_break}";
    }

    /**
     * Gets all classes with FluentExtension
     *
     * @return array Array of base classes and their subclasses with fluent extension
     */
    private function getFluentClasses()
    {
        $all_fluent_classes = [];

        foreach ($this->getBaseFluentClasses() as $base_fluent_class) {
            foreach (ClassInfo::subclassesFor($base_fluent_class) as $base_fluent_subclass) {
                foreach (DataObject::get_extensions($base_fluent_subclass) as $base_fluent_subclass_extension) {
                    if (is_a($base_fluent_subclass_extension, FluentExtension::class, true)) {
                        $all_fluent_classes[] = $base_fluent_subclass;
                        break;
                    }
                }
            }
        }

        return array_unique($all_fluent_classes);
    }

    /**
     * Get unique set of fluent classes
     *
     * @return array Array of base classes with fluent extension
     */
    private function getBaseFluentClasses()
    {
        $base_fluent_classes = [];

        $data_classes = ClassInfo::subclassesFor(DataObject::class);
        array_shift($data_classes);
        foreach ($data_classes as $data_class) {
            $base_data_class = DataObject::getSchema()->baseDataClass($data_class);
            foreach (DataObject::get_extensions($base_data_class) as $extension) {
                if (is_a($extension, FluentExtension::class, true)) {
                    $base_fluent_classes[] = $base_data_class;
                    break;
                }
            }
        }

        return array_unique($base_fluent_classes);
    }

    /**
     * Determine whether the record has been published previously/is currently published
     *
     * @param DataObject $instance
     * @return bool
     */
    private function isPublished(DataObject $instance)
    {
        $isPublished = false;

        if ($instance->hasMethod('isPublished')) {
            $isPublished = $instance->isPublished();
        }

        return $isPublished;
    }

    /**
     * Check if table exists in current database
     *
     * @param String $table Table to check if exists
     * @return bool Whether table exists in database
     */
    private static function dbTableExists($table)
    {
        return DB::query(sprintf("SHOW TABLES LIKE '%s'", $table))->numRecords();
    }

    /**
     * Copy _Versions table data to _Localised_Versions table for all translatable tables
     *
     * This should be run before any instance publishing actions to ensure publishes from
     * these tasks create versions later than any version previously created
     *
     * @param Array $fluent_classes All fluent classes to migrate version data on
     * @return void
     */
    private function copyVersionedDataToLocalisedVersionTable($fluent_classes)
    {
        // Loop translation group set
        foreach ($fluent_classes as $fluent_class) {
            $query_add_to_localised_versions_table = '';

            echo "Generating query for page history data of tables:{$this->line_break}";
            // Store relevant table names for current fluent class
            $fluent_class_table_name = DataObject::getSchema()->tableName($fluent_class);
            $table_versions =  $fluent_class_table_name . '_Versions';
            $table_localised_versions = $fluent_class_table_name . '_Localised_Versions';

            // Continue to next group if a required tables doesn't exist
            foreach ([
                $table_versions,
                $table_localised_versions
            ] as $table) {
                if (!$this->dbTableExists($table)) {
                    echo "Table not found: {$table}{$this->line_break}";
                    echo "{$this->line_break}";
                    continue;
                }
            }

            echo " - from table {$table_versions}{$this->line_break}";
            echo " - to table {$table_localised_versions}{$this->line_break}";

            // Get _Localised_Versioned field names minus ID field
            // ID is auto-incremented on the _Localised_Versions table we'll be inserting into
            $table_fields = $this->getFieldNamesFromTable($table_localised_versions, ['ID']);
            // Continue to next fluent class if no fields found
            if (empty($table_fields)) {
                echo "No fields found for table: {$table_localised_versions}{$this->line_break}";
                echo "{$this->line_break}";
                continue;
            }

            // Remove ambiguity from fields for query
            // All fields should be from current _Versions table except 'Locale' which should be from SiteTree
            $table_localised_versions_fields = array_map(
                function ($field) use ($table_versions) {
                    return $field == 'Locale'
                        ? "SiteTree.{$field}"
                        : "{$table_versions}.{$field}";
                },
                $table_fields
            );

            // Make sql query ready representation of _Localised_Versions table fields
            $table_localised_versions_fields_for_query = implode(', ', $table_localised_versions_fields);

            // Get all _Versions table records incuding each record's Locale
            $table_versions_records = DB::query(
                sprintf(
                    "
                        SELECT %s
                        FROM %s
                        LEFT JOIN SiteTree
                        ON %s.RecordID = SiteTree.ID
                        ORDER BY %s.ID;
                    ",
                    $table_localised_versions_fields_for_query,
                    $table_versions,
                    $table_versions,
                    $table_versions
                )
            );

            if ($table_versions_records->numRecords() === 0) {
                echo "No _Versions records found for table '{$table_versions}'{$this->line_break}";
                echo "{$this->line_break}";
                continue;
            }

            foreach ($table_versions_records as $key => $table_version_record) {
                $query_add_to_localised_versions_table = sprintf(
                    "
INSERT INTO %s (%s)
VALUES ",
                    $table_localised_versions,
                    implode(', ', $table_fields)
                );

                // Reorder fields to use in query to match that of the table we're inserting into
                $fields_to_insert = '';
                foreach ($table_fields as $key => $field) {
                    // Add comma for each new column
                    if ($key != 0 && $key <= count($table_fields)) {
                        $fields_to_insert .= ', ';
                    }

                    // Format data based on field data type
                    switch (strtolower(gettype($table_version_record[$field]))) {
                        case "null": {
                                $fields_to_insert .= 'null';
                                break;
                            }
                        case "integer": {
                                $fields_to_insert .= $table_version_record[$field];
                                break;
                            }
                        case "string": { }
                        default: {
                                $fields_to_insert .= sprintf(
                                    '%s',
                                    Convert::raw2sql($table_version_record[$field], true)
                                );
                            }
                    }
                }

                // Add values to insert query
                $query_add_to_localised_versions_table .= sprintf(
                    "
    (%s)",
                    $fields_to_insert
                );

                try {
                    // Unfortunately only 1 record can be inserted at a time due to connection failures during bigger queries
                    // It's less efficient but more reliable
                    DB::query($query_add_to_localised_versions_table);
                } catch (DatabaseException $e) {
                    echo "{$this->line_break}";
                    echo "Error inerting records:{$this->line_break}";
                    var_dump($e->getMessage());
                    echo "{$this->line_break}";
                }
            }

            echo "All records for {$fluent_class} table migrated from to _Version to Localised_Versions{$this->line_break}";
            echo "{$this->line_break}";
        }
    }

    /**
     * Migrate all translation groups
     *
     * Translation groups are created when a new page is created from an existing page via the
     * Translations tab. Each new page created on the Translations tab of an existing page gets
     * the same TranslationGroupID as the original page but has it's page id set as the OriginalID.
     * You'll see many OriginalID entries to a single TranslationGroupID and the
     * TranslationGroupID will be the same as the First page id that was created.
     *
     * @param Array  $translation_groups
     * @return Array $sitetree_records_to_exclude_from_delete ID from all records to not delete from SiteTree table
     */
    private function migrateTranslationGroups($translation_groups)
    {
        $sitetree_records = [];

        echo "In migrateTranslationGroups{$this->line_break}";
        echo "{$this->line_break}";

        foreach ($translation_groups as $translation_group) {
            echo "Base table: {$translation_group['base_table']}{$this->line_break}";
            echo "Base translation group table: {$translation_group['translation_group_table']}{$this->line_break}";
            echo "Class: {$translation_group['fluent_class']}{$this->line_break}";
            if (count($translation_group['translation_group_ids']) === 0) {
                echo "No IDs in this translation group{$this->line_break}";
                echo "{$this->line_break}";
                continue;
            }
            echo "{$this->line_break}";

            foreach ($translation_group['translation_group_ids'] as $translation_group_id) {
                echo "Current Translation Group ID: {$translation_group_id}{$this->line_break}";
                echo "{$this->line_break}";

                $translation_group_set = [];

                // Get all original ids (page ids) in a given translation group
                $old_instance_record_ids = DB::query(
                    sprintf(
                        "
                            SELECT OriginalID
                            FROM %s
                            WHERE TranslationGroupID = %d
                        ",
                        $translation_group['translation_group_table'],
                        $translation_group_id
                    )
                )
                    ->column();

                // Get all fluent instances i.e. pages or objects
                $fluent_instances = $translation_group['fluent_class']::get()
                    ->sort('Created')
                    ->byIDs($old_instance_record_ids);

                // Go to next fluent class if no fluent instances
                if (!$fluent_instances->count()) {
                    continue;
                }

                $fluent_instance_count = $fluent_instances->count();
                $fluent_instance_column = $fluent_instances->column();
                echo sprintf(
                    "%d Fluent class %s found in from page IDs of %s: [%s]",
                    $fluent_instance_count,
                    $fluent_instance_count >= 2 ? 'instances' : 'instance',
                    implode(', ', $old_instance_record_ids),
                    implode(', ', $fluent_instance_column)
                );
                echo "{$this->line_break}";
                echo "-----------------------------------------------------------------------{$this->line_break}";

                $translation_group_set = $this->getTranslationGroupSet(
                    $fluent_instances,
                    $translation_group['base_table']
                );

                if (count($translation_group_set) === 0) {
                    echo "No records to update{$this->line_break}";
                    continue;
                }

                // Capture default locale page id for the current translation group
                if (array_key_exists($this->getDefaultLocale(), $translation_group_set)) {
                    $default_instance_record_id = $translation_group_set[$this->getDefaultLocale()]->ID;
                } else {
                    $default_instance_record_id = reset($translation_group_set)->ID; // Just use the first one

                    echo "**IDs added to the SiteTree deletion exceptions list:{$this->line_break}";
                    foreach ($fluent_instance_column as $fluent_instance_id) {
                        if (isset($sitetree_records['deletion_exceptions'][$fluent_instance_id])) {
                            continue;
                        }
                        $sitetree_records['deletion_exceptions'][] = $fluent_instance_id;
                        echo " - {$fluent_instance_id}{$this->line_break}";
                    }
                }

                // Ensure the default instance in this set is published regardless of whether it has translations
                $this->publishInstanceFromTranslationGroup($translation_group_set, $default_instance_record_id);

                // Remove default id if it exists in ids-to-update list
                $old_instance_record_ids = array_filter(
                    $old_instance_record_ids,
                    function ($id) use ($default_instance_record_id) {
                        if ($id != $default_instance_record_id) {
                            return $id;
                        }
                    }
                );

                // Old instance record ids must have contained only the default locale record id
                if (count($old_instance_record_ids) === 0) {
                    echo "No records to sync{$this->line_break}";
                    echo "{$this->line_break}";
                    echo "{$this->line_break}";
                    continue;
                }

                $table_to_delete_from = DataObject::getSchema()->tableName($translation_group['fluent_class']);
                echo "translation group fluent class: {$translation_group['fluent_class']}{$this->line_break}";
                echo "table to delete from: {$table_to_delete_from}{$this->line_break}";
                $sitetree_records['to_delete'][$table_to_delete_from] = $old_instance_record_ids;

                $tables_for_sync_RecordID = $this->getFluentTablesForSync(
                    $translation_group_set,
                    $translation_group['base_table']
                );

                $this->syncTablesForMigratedRecords(
                    $tables_for_sync_RecordID,
                    $default_instance_record_id,
                    $old_instance_record_ids,
                    $translation_group['base_table']
                );
            }

            echo "{$this->line_break}";
        }

        return $sitetree_records;
    }

    /**
     * Map current transition group records into new array with record locale as the key
     *
     * Process all fluent records to map pages associated to the current translation group id
     * into `$translation_group_set` associatively with the current page locale as the key
     *
     * @param ArrayList $fluent_instances Fluent records in a translation group
     * @param String    $base_table       Name of base table name for this translation group set
     * @return Array $translation_group_set Map of the translationgroups table for the current translation group id
     */
    private function getTranslationGroupSet($fluent_instances, $base_table)
    {
        $translation_group_set = [];

        foreach ($fluent_instances as $fluent_instance) {
            /* @var DataObject $fluent_instance */

            // Get the Locale column directly from the base table, because the SS ORM will set it to the default
            $fluent_instance_locale = SQLSelect::create()
                ->setFrom("\"{$base_table}\"")
                ->setSelect('"Locale"')
                ->setWhere(["\"{$base_table}\".\"ID\"" => $fluent_instance->ID])
                ->execute()
                ->first();

            // Ensure that we got the Locale out of the base table before continuing
            if (empty($fluent_instance_locale['Locale'])) {
                echo "Skipping [{$fluent_instance->ID}] {$fluent_instance->Title}: couldn't find Locale{$this->line_break}";
                continue;
            }

            // Check for obsolete classes that don't need to be handled any more
            if ($fluent_instance->ObsoleteClassName) {
                echo "Skipping [{$fluent_instance->ID}] {$fluent_instance->ClassName}: from an obsolete class{$this->line_break}";
                continue;
            }

            /*
             * Should contain a page per locale
             * This should map to the translationgroups table for the current translation group id
             *  * $translation_group_set[Locale] = PageID
             *  * $translation_group_set[OriginalID] = PageID
             */
            $translation_group_set[$fluent_instance_locale['Locale']] = $fluent_instance;
        }

        return $translation_group_set;
    }

    /**
     * Get field names from a given table
     *
     * @param String $table             Table to get fields from
     * @param Array  $fields_to_exclude List of field names to exdlude
     * @return Array|bool Table fields of given table else false if table not found
     */
    private function getFieldNamesFromTable($table, $fields_to_exclude = null)
    {
        if (!$this->dbTableExists($table)) {
            return false;
        }

        $field_names = DB::query(
            sprintf(
                "
                    SHOW COLUMNS
                    FROM  %s
                ",
                $table
            )
        )
            ->column('Field');

        if ($fields_to_exclude === null || count($fields_to_exclude) === 0) {
            return $field_names;
        }

        return array_filter(
            $field_names,
            function ($field) use ($fields_to_exclude) {
                if (!in_array($field, $fields_to_exclude)) {
                    return $field;
                }
            }
        );
    }

    /**
     * Attempt to publish all records into their respective locale
     *
     * @param Array  $translation_group_set    List of records to attempt to publish
     * @param String $default_instance_record_id Default
     * @return void
     */
    private function publishInstanceFromTranslationGroup($translation_group_set, $default_instance_record_id)
    {
        foreach ($translation_group_set as $locale => $instance) {
            echo "Updating '{$instance->ClassName}' '{$instance->Title}' ({$instance->ID}) [RecordID: {$default_instance_record_id}] with locale {$locale} {$this->line_break}";
            // Use Fluent's ORM to write and/or publish the record into the correct locale from Translatable
            FluentState::singleton()
                ->withState(
                    function (FluentState $state) use ($locale, $instance) {
                        $state->setLocale($locale);

                        if (!$this->isPublished($instance)) {
                            $instance->write();
                            echo "  --  Saved to draft";
                        } elseif ($instance->publishRecursive() === false) {
                            echo "  --  Publishing FAILED";
                            throw new Exception("Failed to publish");
                        } else {
                            echo "  --  Published";
                        }
                        echo "{$this->line_break}";
                    }
                );
        }
    }

    /**
     * Get fluent database tables to be synced
     *
     * @param Array  $translation_group_set
     * @param String $base_table            Name of base table name for this translation group set
     * @return void
     */
    private function getFluentTablesForSync($translation_group_set, $base_table)
    {
        $fluent_table_names = array_map(
            function ($table_for_sync) {
                if ($table_for_sync->ClassName ?? false) {
                    return DataObject::getSchema()->tableName($table_for_sync->ClassName);
                } else {
                    return false;
                }
            },
            $translation_group_set
        );

        $fluent_tables_for_sync = array_merge(
            [$base_table],
            $fluent_table_names
        );

        $fluent_tables_for_sync = array_filter(array_unique($fluent_tables_for_sync));

        echo "Fluent classes for sync:{$this->line_break}";
        foreach ($fluent_tables_for_sync as $fluent_table_to_sync) {
            echo " - '{$fluent_table_to_sync}'{$this->line_break}";
        }

        return $fluent_tables_for_sync;
    }

    /**
     * Tables to be synced to unify all translations under one base record
     * Relies on having published via FluentState for data to be in these tables
     * Records not configured to be translated will not be added to these tables
     *
     * @param Array  $tables_for_sync_RecordID
     * @param String $default_instance_record_id
     * @param Array  $old_instance_record_ids
     * @param String $base_table
     * @return void
     */
    private function syncTablesForMigratedRecords(
        $tables_for_sync_RecordID,
        $default_instance_record_id,
        $old_instance_record_ids,
        $base_table
    ) {
        echo "Sync Locale and Version Tables{$this->line_break}";
        echo "Base table: {$base_table}{$this->line_break}";
        echo "Default locale record id: {$default_instance_record_id}{$this->line_break}";
        echo "Old instance record ids: " . implode(', ', $old_instance_record_ids) . "{$this->line_break}";
        // Tables to sync RecordID
        $tables_to_sync_RecordID = [];
        foreach ($tables_for_sync_RecordID as $table_for_sync_RecordID) {
            foreach ([
                '_Localised',
                '_Localised_Live',
                '_Localised_Versions',
            ] as $table_suffix) {
                $tables_to_sync_RecordID[] = $table_for_sync_RecordID . $table_suffix;
            }
        }
        // Only base table should have it's versions updated as Page subclass tables fail on duplicated key
        $tables_to_sync_RecordID[] = $this->getBaseTable() . '_Versions';
        sort($tables_to_sync_RecordID);
        echo "Syncing table record ids:{$this->line_break}";
        foreach ($tables_to_sync_RecordID as $table_to_sync_RecordID) {
            if (!$this->dbTableExists($table_to_sync_RecordID)) {
                echo "Table doesn't exist in database: {$table_to_sync_RecordID}{$this->line_break}";
                continue;
            }
            DB::query(
                sprintf(
                    "
                        UPDATE %s
                        SET RecordID = %d
                        WHERE RecordID IN (%s)
                    ",
                    $table_to_sync_RecordID,
                    $default_instance_record_id,
                    implode(', ', $old_instance_record_ids)
                )
            );
            echo " - synced RecordIDs on table: '{$table_to_sync_RecordID}'{$this->line_break}";
            echo "    - from: " . implode(', ', $old_instance_record_ids) . "{$this->line_break}";
            echo "    - to:   {$default_instance_record_id}{$this->line_break}";
        }
        echo "Successfully synced RecordIDs{$this->line_break}";

        // Tables to sync ParentID
        $tables_to_sync_ParentID = [];
        foreach ([
            '',
            'Link',
            '_Live',
            '_Versions',
        ] as $table_suffix) {
            $tables_to_sync_ParentID[] = $base_table . $table_suffix;
        }
        sort($tables_to_sync_ParentID);
        echo "Syncing table ParentIDs:{$this->line_break}";
        foreach ($tables_to_sync_ParentID as $table_to_sync_ParentID) {
            if (!$this->dbTableExists($table_to_sync_ParentID)) {
                continue;
            }
            DB::query(
                sprintf(
                    "
                        UPDATE %s
                        SET ParentID = %d
                        WHERE ParentID IN (%s)
                    ",
                    $table_to_sync_ParentID,
                    $default_instance_record_id,
                    implode(', ', $old_instance_record_ids)
                )
            );
            echo " - synced ParentIDs on table: '{$table_to_sync_ParentID}'{$this->line_break}";
            echo "    - from: " . implode(', ', $old_instance_record_ids) . "{$this->line_break}";
            echo "    - to:   {$default_instance_record_id}{$this->line_break}";
        }
        echo "Successfully synced ParentIDs{$this->line_break}";
        echo "Successfully synced tables in this set{$this->line_break}";
        echo "{$this->line_break}";
        echo "{$this->line_break}";
    }

    /**
     * Prune old translatable database things
     *
     * @param Array $sitetree_records['deletion_exceptions'] All records to not delete from SiteTree tables
     * @return void
     */
    private function deleteOldBaseItemsFromTables($sitetree_records)
    {
        // Flag tables to prune old translations from
        $deletion_table_suffixes = [
            '',
            '_Live',
            '_Versions',
        ];

        echo "Deletion exceptions IDs:{$this->line_break}";
        foreach ($sitetree_records['deletion_exceptions'] as $sitetree_record_deletion_exception) {
            echo " - {$sitetree_record_deletion_exception}{$this->line_break}";
        }
        echo "{$this->line_break}";

        echo "Deleting records where Locale is not '{$this->getDefaultLocale()}' from tables:{$this->line_break}";
        // Delete old base items that don't have the default locale
        foreach ($deletion_table_suffixes as $deletion_table_suffix) {
            $base_table_to_delete = $this->getBaseTable() . $deletion_table_suffix;
            // Don't attempt to query further if table doesn't exist
            if (!$this->dbTableExists($base_table_to_delete)) {
                continue;
            }
            // Don't attempt to query further if Locale column doesn't exist
            if (!DB::query(sprintf("SHOW COLUMNS FROM %s LIKE 'Locale'", $base_table_to_delete))->numRecords()) {
                continue;
            }

            $record_deletion_query = "DELETE FROM %s WHERE Locale != '%s'";

            if (count(array_filter($sitetree_records['deletion_exceptions'])) === 0) {
                DB::query(
                    sprintf(
                        $record_deletion_query,
                        $base_table_to_delete,
                        $this->getDefaultLocale()
                    )
                );
            } else {
                DB::query(
                    sprintf(
                        $record_deletion_query .= " AND ID NOT IN (%s)",
                        $base_table_to_delete,
                        $this->getDefaultLocale(),
                        implode(', ', $sitetree_records['deletion_exceptions'])
                    )
                );
            }
            echo " - {$base_table_to_delete}{$this->line_break}";
        }
        echo "{$this->line_break}";

        echo "Deleting records from tables:{$this->line_break}";
        foreach ($sitetree_records['to_delete'] as $table => $ids) {
            foreach ($deletion_table_suffixes as $deletion_table_suffix) {
                $base_table_to_delete = $table . $deletion_table_suffix;

                if (!$this->dbTableExists($base_table_to_delete)) {
                    continue;
                }

                echo " - {$base_table_to_delete}{$this->line_break}";

                $ids_to_delete = implode(', ', $ids);
                // delete records from table where that page has been merged into the master page
                DB::query(
                    sprintf(
                        "
                            DELETE FROM %s
                            WHERE ID IN (%s)
                        ",
                        $base_table_to_delete,
                        $ids_to_delete
                    )
                );
                echo " - - $ids_to_delete{$this->line_break}";
            }
        }

        echo "{$this->line_break}";
    }

    /**
     * Remove all
     *
     * @return void
     */
    private function cleanTranslatableTables()
    {
        echo "Dropping Translatable tables from database:{$this->line_break}";
        foreach (DB::query("SHOW TABLES LIKE '%_translationgroups'")->column() as $translationgroups_table) {
            if (!$this->dbTableExists($translationgroups_table)) {
                echo "Table doesn't exist: {$translationgroups_table}{$this->line_break}";
                continue;
            }
            DB::query(sprintf('DROP TABLE IF EXISTS "%s"', $translationgroups_table));
            echo " - '{$translationgroups_table}'{$this->line_break}";
        }
        echo "{$this->line_break}";
    }
}

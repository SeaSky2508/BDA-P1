/////////////////////////////////////////////////////////////
//
// pgAdmin 4 - PostgreSQL Tools
//
// Copyright (C) 2013 - 2024, The pgAdmin Development Team
// This software is released under the PostgreSQL Licence
//
//////////////////////////////////////////////////////////////



define('pgadmin.browser.utils',
  ['sources/pgadmin'], function(pgAdmin) {

  let pgBrowser = pgAdmin.Browser = pgAdmin.Browser || {};

  pgBrowser['MainMenus'] = [];

  pgAdmin['csrf_token_header'] = 'X-pgA-CSRFToken';
  pgAdmin['csrf_token'] = 'IjQyMTNjZTVkMjhhN2Q5NjU2ZTM3N2FjN2IxNTg0NDE3ODUyNWNmMWYi.ZigmcQ.bjE_Yr1Dq9ZkJSp2_PCOdm4zfBo';
  pgAdmin['server_mode'] = 'True';

  /* Get the inactivity related config */
  pgAdmin['user_inactivity_timeout'] = 0;
  pgAdmin['override_user_inactivity_timeout'] = 'True' == 'True';

  /* GET PSQL Tool related config */
  pgAdmin['enable_psql'] =  'False' == 'True';
  pgAdmin['shared_storage'] = []
  pgAdmin['restricted_shared_storage'] = []
  pgAdmin['platform'] =  'linux';
  pgAdmin['qt_default_placeholder'] = '%DATABASE%/%USERNAME%@%SERVER%'
  pgAdmin['vw_edt_default_placeholder'] = '%SCHEMA%.%TABLE%/%DATABASE%/%USERNAME%@%SERVER%'

  /* GET Binary Path Browse config */
  pgAdmin['enable_binary_path_browsing'] = 'False' == 'True';

  pgAdmin['fixed_binary_paths'] = {'pg': '', 'pg-12': '', 'pg-13': '', 'pg-14': '', 'pg-15': '', 'pg-16': '', 'ppas': '', 'ppas-12': '', 'ppas-13': '', 'ppas-14': '', 'ppas-15': '', 'ppas-16': ''};

  /* GET the pgadmin server's locale */
  pgAdmin['pgadmin_server_locale'] =  '';

   /* Server Heartbeat Timeout */
  pgAdmin['heartbeat_timeout'] =  '30';

  /* Minimum password length */
  pgAdmin['password_length_min'] = '6';

  /* Enable server password exec command */
  pgAdmin['enable_server_passexec_cmd'] = 'False';

  // Define list of nodes on which Query tool option doesn't appears
  let unsupported_nodes = pgAdmin.unsupported_nodes = [
     'server_group', 'server', 'coll-tablespace', 'tablespace',
     'coll-role', 'role', 'coll-resource_group', 'resource_group',
     'coll-database', 'coll-pga_job', 'coll-pga_schedule', 'coll-pga_jobstep',
     'pga_job', 'pga_schedule', 'pga_jobstep',
     'coll-replica_nodes', 'replica_nodes'
  ];

  pgBrowser.utils = {
    layout: '{"dockbox":{"id":"+2","size":200,"mode":"horizontal","children":[{"id":"+3","size":395,"tabs":[{"id":"id-object-explorer"}],"group":"object-explorer","activeId":"id-object-explorer"},{"id":"id-main","size":1524,"tabs":[{"id":"id-dashboard"},{"id":"id-properties"},{"id":"id-sql"},{"id":"id-statistics"},{"id":"id-dependencies"},{"id":"id-dependents"},{"id":"id-processes"},{"id":"id-erd-tool_2510927"},{"id":"id-query-tool_8262820"}],"group":"playground","activeId":"id-query-tool_8262820"}]},"floatbox":{"id":"+7","size":0,"mode":"float","children":[]},"windowbox":{"id":"+8","size":0,"mode":"window","children":[]},"maxbox":{"id":"+9","size":1,"mode":"maximize","children":[]}}',
    pg_help_path: 'https://www.postgresql.org/docs/$VERSION$/',
    tabSize: '4',
    wrapCode: 'False' == 'True',
    useSpaces: 'False',
    insertPairBrackets: 'True' == 'True',
    braceMatching: 'True' == 'True',
    is_indent_with_tabs: 'True' == 'True',
    app_name: 'pgAdmin 4',
    app_version_int: '80500',
    pg_libpq_version: 0,
    support_ssh_tunnel: 'True' == 'True',
    logout_url: '/pgadmin4/logout?next=/pgadmin4/browser/',

    counter: {total: 0, loaded: 0},
    registerScripts: function (ctx) {
      // There are some scripts which needed to be loaded immediately,
      // but - not all. We will will need to generate all the menus only
      // after they all were loaded completely.
    },

    addBackendMenus: function (obj) {
      // Generate the menu items only when all the initial scripts
      // were loaded completely.
      //
      // First - register the menus from the other
      // modules/extensions.
            obj.add_menus([
      {
  name: "mnu_resetlayout",
  module: pgAdmin.Settings,
  callback: "show",
  label: "Reset Layout", applies: ["file"],
  priority: 998,
  enable: "",
      }]);
            obj.add_menus([
  ]);
            obj.add_menus([
  ]);
            obj.add_menus([
  ]);
            obj.add_menus([
      {
  name: "mnu_quick_search_help",
  url: "#",
  target: "pgadmin_quick_search_help",
  icon: "fa fa-question",
  label: "Quick Search", applies: ["help"],
  priority: 99,
  enable: "",
      },     {
  name: "mnu_online_help",
  url: "/pgadmin4/help/help/index.html?ver=80500",
  target: "pgadmin_help",
  icon: "fa fa-question",
  label: "Online Help", applies: ["help"],
  priority: 100,
  enable: "",
      },     {
  name: "mnu_pgadmin_website",
  url: "https://www.pgadmin.org/",
  target: "pgadmin_website",
  icon: "fa fa-external-link-alt",
  label: "pgAdmin Website", applies: ["help"],
  priority: 200,
  enable: "",
      },     {
  name: "mnu_postgresql_website",
  url: "https://www.postgresql.org/",
  target: "postgres_website",
  icon: "fa fa-external-link-alt",
  label: "PostgreSQL Website", applies: ["help"],
  priority: 300,
  enable: "",
      },     {
  name: "mnu_about",
  module: pgAdmin.About,
  callback: "about_show",
  icon: "fa fa-info-circle",
  label: "About pgAdmin 4", applies: ["help"],
  priority: 999,
  enable: "",
      }]);
          },

        userMenuInfo: {
      username: 'pere.mateu03@gmail.com',
      auth_source: 'internal',
      gravatar: 'https://secure.gravatar.com/avatar/d56a3cd93d5c2df48f88497166f7a442?s=100&d=retro&r=g',
      menus: [
                {
          label: 'Change Password',
          type: 'normal',
          callback: ()=>{
            pgAdmin.UserManagement.change_password(
              '/pgadmin4/browser/change_password'
            )
          }
        },
        {
          type: 'separator',
        },
                        {
          label: 'Two-Factor Authentication',
          type: 'normal',
          callback: ()=>{
            pgAdmin.UserManagement.show_mfa(
              '/pgadmin4/mfa/register?next=internal'
            )
          }
        },
        {
          type: 'separator',
        },
                        {
          label: 'Users',
          type: 'normal',
          callback: ()=>{
            pgAdmin.UserManagement.show_users()
          }
        },
        {
          type: 'separator',
        },
                {
          label: 'Logout',
          type: 'normal',
          callback: ()=>{
            window.location="/pgadmin4/logout?next=/pgadmin4/browser/";
          }
        },
      ],
    },
      };
  return pgBrowser;
});
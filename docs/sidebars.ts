import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  guideSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Guide',
      collapsed: false,
      items: [
        'guide/quickstart',
        'guide/setup',
        'guide/authentication',
        'guide/architecture',
        'guide/web-ui',
        'guide/clone',
        'guide/advanced-clone',
        'guide/diff-and-compare',
        'guide/sync',
        'guide/rollback',
        'guide/safety',
        'guide/governance',
        'guide/scheduling',
        'guide/analytics',
        'guide/validation',
        'guide/monitoring',
        'guide/notebooks',
        'guide/storage-metrics',
        'guide/create-job',
        'guide/desktop',
        'guide/databricks-app',
        'guide/cicd',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: [
        'reference/cli',
        'reference/api',
        'reference/configuration',
        'reference/faq',
        'reference/changelog',
      ],
    },
  ],
};

export default sidebars;

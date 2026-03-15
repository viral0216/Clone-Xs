import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Clone-Xs',
  tagline: 'Unity Catalog clone utility for Databricks',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://viral0216.github.io',
  baseUrl: '/clone-xs/',

  organizationName: 'viral0216',
  projectName: 'clone-xs',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Clone-Xs',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'guideSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/viral0216/clone-xs',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Getting Started', to: '/docs/intro'},
            {label: 'Authentication', to: '/docs/guide/authentication'},
            {label: 'CLI Reference', to: '/docs/reference/cli'},
          ],
        },
        {
          title: 'Resources',
          items: [
            {label: 'PyPI', href: 'https://pypi.org/project/clone-xs/'},
            {label: 'Databricks Docs', href: 'https://docs.databricks.com/en/data-governance/unity-catalog/index.html'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/viral0216/clone-xs'},
            {label: 'FAQ', to: '/docs/reference/faq'},
          ],
        },
      ],
      copyright: `Copyright \u00a9 ${new Date().getFullYear()} Clone-Xs Contributors. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'yaml', 'python', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;

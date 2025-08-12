import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeFlexoki from 'starlight-theme-flexoki'

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Open Compute Framework',
			plugins: [starlightThemeFlexoki()],
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/researchcomputer/OpenComputeFramework' }],
			sidebar: [
				{
					label: 'Guides',
					items: [
						{ label: "Introduction", link: "/guides/intro/" },
						{ label: 'ML Inference', link: '/guides/ml_inference/' },
						{ label: 'Deployment', link: '/guides/spinup/' },
					],
					},
					{
						label: 'Reference',
						items: [
							{ label: 'Architecture', link: '/reference/architecture/' },
							{ label: 'CLI', link: '/reference/cli/' },
							{ label: 'API', link: '/reference/api/' },
							{ label: 'Configuration', link: '/reference/configuration/' },
						],
					}

			],
		}),
	],
});

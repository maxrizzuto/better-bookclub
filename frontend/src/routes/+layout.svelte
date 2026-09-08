<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	// import '../reset.css';
	import '../app.css'

	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<link rel="preconnect" href="https://fonts.googleapis.com" />
	<link rel="preconnect" href="https://fonts.gstatic.com" />
</svelte:head>

{@render children()}

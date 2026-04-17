<script lang="ts">
	import UserBook from '$lib/components/UserBook.svelte';
	import type { PageProps } from './$types';
	let { data }: PageProps = $props();
</script>

<div id="page-container">
	<h1 class="page-title">Recs for {data.usernames.join(', ')}</h1>
	<h2>Group Results</h2>
	<div class="results">
		{#each data.group_results as result}
			<div class="book">
				<p>{result.title}</p>
				<p>{result.preds}</p>
				<UserBook isbn={result.isbn13} />
			</div>
		{/each}
	</div>
	<h2>User Results</h2>
	<div class="users">
		{#each Object.entries(data.user_results) as [user, results]}
			<div class="user-container">
				<h3>{user} Results</h3>
				<div class="results">
					{#each results as result}
						<div class="book">
							<p>{result.title}</p>
							<UserBook isbn={result.isbn13} />
							<p>{result.preds}</p>
							<p>{result.isbn}</p>
						</div>
					{/each}
				</div>
			</div>
		{/each}
	</div>
</div>

<style>
	/*GLOBALS*/
	* {
		font-family: 'Oranienbaum', 'Forum', serif;
		box-sizing: border-box;
	}

	:global(body) {
		min-height: 100vh;
		background: #fcffc7;
		background: linear-gradient(
			137deg,
			rgba(252, 255, 199, 1) 0%,
			rgba(228, 199, 235, 1) 50%,
			rgba(188, 224, 182, 1) 100%
		);
	}

	#page-container {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		padding: 0px 15vw;
	}

	.page-title {
		font-size: 2.5em;
		font-weight: bold;
		font-style: italic;
		margin-top: 5vh;
		margin-bottom: 15px;
	}

	/* RESULTS */
	.results {
		display: flex;
		max-width: 50px;
	}
	h2 {
		font-size: 2em;
		margin-top: 20px;
		margin-bottom: 15px;
		border-bottom: 1px solid black;
	}
	h3 {
		font-size: 1.75em;
		margin-bottom: 10px;
	}

	.user-container {
		margin-left: 20px;
	}

	.book {
		margin: 0 15px;
		width: 100px;
	}
</style>

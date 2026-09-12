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
				{#if result.olid !== null}
					<UserBook id={result.olid} />
				{:else}
					<UserBook id={result.isbn13} />
				{/if}
				<div class="book-text">
					<span class="rating">{Math.round(result.preds * 100)}%</span>
					<span class="title">{result.title}</span>
				</div>
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
							{#if result.olid != null}
								<UserBook id={result.olid} />
							{:else}
								<UserBook id={result.isbn13} />
							{/if}
							<div class="book-text">
								<span class="rating">{Math.round(result.preds * 100)}%</span>
								<span class="title">{result.title}</span>
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/each}
	</div>
</div>

<style>
	#page-container {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		padding: 0px 15vw;
	}

	.page-title {
		font-size: 2.5em;
		font-weight: 100;
		margin-top: 5vh;
		margin-bottom: 15px;
	}

	/* RESULTS */
	.results {
		display: flex;
		flex-wrap: wrap;
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

	.book {
		margin: 0 15px;
		display: flex;
		flex-direction: column;
		width: 140px;
		/*width: 75px;*/
	}

	.book-text {
		margin-bottom: 20px;
	}

	.rating {
		color: #b2c3e9;
	}
</style>

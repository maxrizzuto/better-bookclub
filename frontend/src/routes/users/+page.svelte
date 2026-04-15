<script>
	import { enhance } from '$app/forms';
	import UserBook from '$lib/components/UserBook.svelte';
	import { userBooks } from '../state.svelte';
</script>

<h1>Page w/ users' books streamed in</h1>

{#each Object.entries(userBooks.books) as [username, books]}
	<div class="user">
		<h1>{username}</h1>
		{#each books as book}
			<UserBook isbn={book.isbn} />
		{/each}
	</div>
{/each}

<form method="POST" name="booksForm" use:enhance>
	<button type="submit">Submit</button>
</form>

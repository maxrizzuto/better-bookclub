<script lang="ts">
	import Loading from '$lib/components/Loading.svelte';
	import Recs from '$lib/components/Recs.svelte';
	import UserForm from '$lib/components/UserForm.svelte';
	import type { Book } from '$lib/types.js';

	let results: Book[] = $state([]);
	let unames: String[] = $state([]);
	let phase = $state('form');
	let { form } = $props();

	function onSubmitStart(data_unames: string[]) {
		unames = data_unames;
		phase = 'loading';
	}

	// [TODO] make <Book> data type for returned data
	function onSubmitEnd(data: Book[]) {
		results = data;
		phase = 'results';
	}
</script>

{#if phase === 'form'}
	<UserForm {onSubmitStart} {onSubmitEnd} />
{:else if phase === 'loading'}
	<Loading {unames} />
{:else if phase === 'results'}
	<Recs {unames} {results} />
{/if}

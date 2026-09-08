<script lang="ts">
	import { enhance } from '$app/forms';
	import { flip } from 'svelte/animate';
	import { fly } from 'svelte/transition';
	import type { PageProps } from './$types';
	import UserBook from '$lib/components/UserBook.svelte';
	import { onMount } from 'svelte';
	import { invalidateAll } from '$app/navigation';

	type Book = {
		title: string;
		storygraph_id: string;
		isbn13: string;
		shelf?: string;
		rating?: number;
	};

	type UserBooks = Record<string, Array<Book>>;

	const users: string[] = $state([]);
	const userBooks: UserBooks = $state({});
	let { form }: PageProps = $props();
	let eventSource: EventSource | null = null;
	let formSubmitted: boolean = $state(false);

	function getUserBooks() {
		formSubmitted = true;
		if (eventSource) return;

		const query = users.map((uname) => `user=${uname}`).join('&');
		eventSource = new EventSource(`http://127.0.0.1:8000/users?${query}`);

		eventSource.onmessage = (event: MessageEvent) => {
			const data = JSON.parse(event.data);
			if (data.data === 'close') return;
			console.log(data);
			if (data.username in userBooks) {
				userBooks[data.username].push(...data.books);
			} else {
				userBooks[data.username] = data.books;
			}
		};

		eventSource.addEventListener('close', () => {
			eventSource?.close();
			eventSource = null;
		});

		eventSource.onerror = () => {
			eventSource?.close();
			eventSource = null;
		};

		$inspect(userBooks);
	}

	// [TODO] function for add user, export serverside function that checks user (not form action)
	function addUser() {
		const uname = (document.getElementById('uname') as HTMLFormElement).value.trim();
		const errorMessage = document.getElementById('inputError')!;
		errorMessage.textContent = '';
		if (!uname) {
			errorMessage.textContent = 'User input is empty.';
		} else if (users.includes(uname)) {
			errorMessage.textContent = 'User has already been added.';
		} else {
			users.unshift(uname);
		}
		(document.getElementById('uname')! as HTMLFormElement).value = '';
	}

	function removeUser(uname: string) {
		users.splice(users.indexOf(uname), 1);
	}

	function handleEnter(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			e.preventDefault();
			addUser();
		}
	}

	function checkSubmit(e: Event) {
		if (users.length < 2) {
			e.preventDefault();

			return;
		}
	}
</script>

{#if formSubmitted}
	<div class="users-container">
		{#if userBooks}
			{#each Object.entries(userBooks) as [username, books]}
				<div class="user">
					<h1 class="username">{username}</h1>
					<div class="user-books">
						{#each books as book}
							<UserBook isbn13={book.isbn13} />
						{/each}
					</div>
				</div>
			{/each}
		{/if}
	</div>
	<form method="POST" action="?/getRecs">
		<button type="submit">Get recs</button>
		{#each users as user (user)}
			<input type="hidden" name="user" value={user} />
		{/each}
	</form>

	<style>
		.users-container {
			margin-top: 10vh;
		}
		.user {
			display: flex;
			justify-content: center;
			width: 80vw;
			margin: 0 10vw;
			border-bottom: 1px solid antiquewhite;
			align-items: baseline;
		}
		.username {
			justify-self: center;
			font-style: italic;
			font-size: 2em;
		}

		.user-books {
			display: flex;
			justify-content: flex-start;
			align-items: baseline;
			flex: 1;
			flex-wrap: wrap;
		}
	</style>
{:else}
	<div id="page-container">
		<div id="title-block">
			<h1 id="title">Want ideas for your next book club book?</h1>
		</div>

		<div id="form">
			<form
				method="GET"
				name="unamesForm"
				id="unamesForm"
				onsubmit={(e) => {
					e.preventDefault();
					getUserBooks();
				}}
			>
				<div class="inputs">
					<input
						type="text"
						id="uname"
						placeholder="add StoryGraph usernames here"
						onkeydown={handleEnter}
					/>
					<button type="button" id="addUser" onclick={addUser}>+</button>
					<div class="submit-button">
						{#if users.length < 2}
							<button type="submit" id="submit" class="unsubmittable" onclick={checkSubmit}
								>&RightArrow;</button
							>
							<span class="tooltip">Must add at least 2 users to submit</span>
						{:else}
							<button type="submit" id="submit" class="submittable" onclick={checkSubmit}
								>&RightArrow;</button
							>
						{/if}
					</div>
				</div>
				<div id="inputError"></div>
				<div id="users">
					{#each users as user (user)}
						<div
							class="user"
							id={user}

						>
						    <!-- animate:flip={{ duration: 400 }} -->
						    <!-- transition:fly={{ y: -5, duration: 400 }} -->
							<!-- [TODO] add validation icon if uname exists -->
							<input type="hidden" name="user" value={user} />
							<button class="removeUser" type="button" onclick={() => removeUser(user)}>-</button>
							<p>{user}</p>
						</div>
					{/each}
				</div>
			</form>
		</div>
	</div>

	<style>
		/* TITLE */
		#title-block {
			display: flex;
			justify-content: center;
			align-items: flex-end;
			height: 35vh;
			margin-top: 15vh;
		}

		#title {
		    font-weight: 100;
			font-size: 5rem;
			max-width: 70vw;
			text-align: flex-start;
			margin-bottom: 0;
		}

		/* FORM AND INPUTS */

		#form {
			display: flex;
			justify-content: center;
			align-items: center;
			font-size: 1.25rem;
			padding-top: 1vh;
			margin-top: 1vh;
		}

		#inputError {
			color: white;
			font-size: 0.75em;
			margin: 10px 20px;
			height: 0.75em;
		}

		input {
			width: 50vw;
			height: 75px;
			font-size: 1em;
			padding-left: 20px;
			max-width: 1000px;
			text-align: left;
			font-family: OCRA, sans-serif;
		}

		#addUser {
			display: inline-block;
			box-sizing: content-box;
			height: 50px;
			width: 50px;
			margin-left: 10px;
			font-size: 2em;
			background-color: black;
			color: antiquewhite;
			padding: 0px;
			border: 1px solid antiquewhite;
		}

		#addUser:hover {
			background-color: lightgray;
		}

		.inputs {
			display: flex;
			align-items: center;
		}

		#submit {
			display: inline-block;
			height: 50px;
			width: 50px;
			margin-left: 10px;
			background-color: black;
			border: 1px solid antiquewhite;
			padding: 0;
			font-size: 1.5em;
		}

		#submit.unsubmittable {
			background-color: gray;
			color: antiquewhite;
		}

		.tooltip {
			font-size: 0.9em;
			visibility: hidden;
			margin-left: 10px;
		}

		#submit.unsubmittable:hover + .tooltip {
			visibility: visible;
		}

		#submit.submittable {
			cursor: pointer;
			color: antiquewhite;
		}

		#submit.submittable:hover {
			background-color: darkseagreen;
		}

		.submit-button {
			display: flex;
			align-items: center;
		}

		/* ADDED USERS */

		.removeUser {
			color: white;
			background-color: lightcoral;
			font-size: 2em;
			width: 1em;
			height: 1em;
			margin-right: 10px;
			border-radius: 50%;
			display: flex;
			align-items: center;
			justify-content: center;
		}

		.removeUser:hover {
			background-color: red;
		}

		#users {
			display: flex;
			padding-left: 20px;
			width: 100%;
			flex-direction: column;
			flex-wrap: wrap;
		}

		.user {
			padding: 10px 5px;
			flex: 1;
			display: flex;
			align-items: center;
			padding: 20px 0px;
			border-bottom: 1px solid antiquewhite;
			font-family: OCRA, sans-serif;
		}

		.user:last-of-type {
			padding-bottom: 0;
			border-bottom: none;
		}
	</style>
{/if}

<!-- global styles -->
<style>
	/* OVERALL PAGE */
	#page-container {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		padding: 0px 15vw;
	}
</style>
